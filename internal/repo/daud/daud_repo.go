package daud

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

const RUN_COLLECTION = "daud.workflow_run"

const (
	ERR_MISSING_COLLECTION_NAME = "error missing collection name"
	ERR_MISSING_RUN_ID          = "error missing run ID"
	ERR_MISSING_WORKFLOW_ID     = "error missing workflow ID"
	ERR_DUPLICATE_RUN           = "error duplicate run"
	ERR_RUN_CREATE              = "error creating workflow run"
	ERR_NOT_FOUND               = "error no documents found for the given query"
)

var (
	ErrMissingCollectionName = errors.New(ERR_MISSING_COLLECTION_NAME)
	ErrMissingWorkflowId     = errors.New(ERR_MISSING_WORKFLOW_ID)
	ErrMissingRunId          = errors.New(ERR_MISSING_RUN_ID)
	ErrDuplicateRun          = errors.New(ERR_DUPLICATE_RUN)
	ErrDecodeObjectId        = errors.New("error decoding object ID from MongoDB")
	ErrNotFound              = errors.New(ERR_NOT_FOUND)
)

type DaudRepo interface {
	CreateRun(ctx context.Context, params *stores.WorkflowRun) (string, error)
	GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error)
	DeleteRun(ctx context.Context, runId string) error
	Close(ctx context.Context) error
}

type daudRepo struct {
	infra.DBStore
}

func NewDaudRepo(rc infra.DBStore) (*daudRepo, error) {
	return &daudRepo{
		DBStore: rc,
	}, nil
}

func (dr *daudRepo) CreateRun(ctx context.Context, wkflRun *stores.WorkflowRun) (string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting logger from context: %w", err)
	}

	if wkflRun.WorkflowId == "" {
		return "", ErrMissingWorkflowId
	}

	if wkflRun.RunId == "" {
		return "", ErrMissingRunId
	}

	coll := dr.Store().Collection(RUN_COLLECTION)

	now := time.Now()
	wkflRun.CreatedAt = now
	wkflRun.UpdatedAt = now
	wkflRun.DeletedAt = time.Time{} // ensure DeletedAt is zeroed out

	res, err := coll.InsertOne(ctx, wkflRun)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return "", ErrDuplicateRun
		}

		l.Error("CreateRun error", "error", err.Error())
		return "", fmt.Errorf("error creating workflow run: %w", err)
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", ErrDecodeObjectId
	}
	return id.Hex(), err
}

func (dr *daudRepo) GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting logger from context: %w", err)
	}

	if runId == "" {
		return nil, ErrMissingRunId
	}

	coll := dr.Store().Collection(RUN_COLLECTION)
	filter := bson.M{"runId": runId}

	res := coll.FindOne(ctx, filter)

	var wkflRun stores.WorkflowRun
	if err := res.Decode(&wkflRun); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}

		l.Error("GetRun error", "error", err.Error(), "run_id", runId)
		return nil, fmt.Errorf("error fetching workflow run: %w", err)
	}
	return &wkflRun, nil
}

func (dr *daudRepo) DeleteRun(ctx context.Context, runId string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("error getting logger from context: %w", err)
	}

	if runId == "" {
		return ErrMissingRunId
	}

	coll := dr.Store().Collection(RUN_COLLECTION)
	filter := bson.M{"runId": runId}

	res, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		l.Error("DeleteRun error", "error", err.Error(), "run_id", runId)
		return fmt.Errorf("error deleting workflow run: %w", err)
	}

	if res.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}
