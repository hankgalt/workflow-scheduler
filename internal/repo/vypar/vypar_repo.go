package vypar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

const AGENT_COLLECTION = "vypar.agents"
const FILING_COLLECTION = "vypar.filings"

const (
	ERR_MISSING_COLLECTION_NAME = "missing collection name"
)

var (
	ErrMissingCollectionName = errors.New(ERR_MISSING_COLLECTION_NAME)
	ErrDuplicateAgent        = errors.New("agent already exits")
	ErrDuplicateFiling       = errors.New("filing already exists")
	ErrDecodeObjectId        = errors.New("error decoding object ID from MongoDB")
	ErrMissingEntityId       = errors.New("missing entity ID")
	ErrNoAgentFound          = errors.New("no agent found")
	ErrNoFilingFound         = errors.New("no filing found for the given entity ID")
)

type VyparRepo interface {
	AddAgent(ctx context.Context, ag stores.BusinessAgentMongo) (string, error)
	Close(ctx context.Context) error
}

type vyparRepo struct {
	infra.DBStore
}

func NewVyparRepo(ctx context.Context, rc infra.DBStore) (*vyparRepo, error) {
	agIndxs := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "entityId", Value: 1},
				{Key: "firstName", Value: 1},
				{Key: "lastName", Value: 1},
				{Key: "agentType", Value: 1},
			},
			Options: options.Index().SetUnique(true), // Composite unique index
		},
	}

	err := rc.EnsureIndexes(ctx, AGENT_COLLECTION, agIndxs)
	if err != nil {
		return nil, fmt.Errorf("error adding agent indexes: %w", err)
	}

	return &vyparRepo{
		DBStore: rc,
	}, nil
}

func (vr *vyparRepo) GetItemCount(ctx context.Context, collection string) (int64, error) {
	logger, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting logger from context: %w", err)
	}

	logger.Debug("GetItemCount", slog.String("collection", collection))
	if collection == "" {
		return 0, ErrMissingCollectionName
	}

	coll := vr.Store().Collection(collection)

	filter := bson.M{}
	totalCount, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		logger.Error("GetItemCount error", slog.String("collection", collection), slog.String("error", err.Error()))
		return 0, fmt.Errorf("error fetching item count: %w", err)
	}
	return totalCount, nil
}

func (vr *vyparRepo) AddAgent(ctx context.Context, ag stores.BusinessAgentMongo) (string, error) {
	logger, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting logger from context: %w", err)
	}
	logger.Debug("AddAgent", slog.Any("agent", ag))
	if ag.EntityID == 0 {
		return "", ErrMissingEntityId
	}

	coll := vr.Store().Collection(AGENT_COLLECTION)

	now := time.Now()
	ag.CreatedAt = now
	ag.UpdatedAt = now

	res, err := coll.InsertOne(ctx, ag)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return "", ErrDuplicateAgent
		}

		logger.Error("AddAgent error", slog.String("error", err.Error()))
		return "", fmt.Errorf("error adding agent: %w", err)
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", ErrDecodeObjectId
	}
	return id.Hex(), err
}

func (vr *vyparRepo) DeleteAgent(ctx context.Context, entityId uint64) (bool, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("error getting logger from context: %w", err)
	}

	if entityId == 0 {
		return false, ErrMissingEntityId
	}

	coll := vr.Store().Collection(AGENT_COLLECTION)
	filter := bson.M{"entityId": entityId}
	result, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		l.Error("DeleteAgent error", slog.String("error", err.Error()))
		return false, fmt.Errorf("error deleting agent: %w", err)
	}
	return result.DeletedCount > 0, nil
}

func (vr *vyparRepo) GetAgent(ctx context.Context, entityId uint64) (*stores.BusinessAgentMongo, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting logger from context: %w", err)
	}

	if entityId == 0 {
		return nil, ErrMissingEntityId
	}

	coll := vr.Store().Collection(AGENT_COLLECTION)
	filter := bson.M{"entityId": entityId}

	var agent stores.BusinessAgentMongo
	err = coll.FindOne(ctx, filter).Decode(&agent)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNoAgentFound
		}
		l.Error("GetAgent error", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error getting agent: %w", err)
	}
	return &agent, nil
}

func (vr *vyparRepo) AddFiling(ctx context.Context, bf stores.BusinessFilingMongo) (string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting logger from context: %w", err)
	}

	l.Debug("AddFiling", slog.Any("filing", bf))

	coll := vr.Store().Collection(FILING_COLLECTION)

	now := time.Now()
	bf.CreatedAt = now
	bf.UpdatedAt = now

	res, err := coll.InsertOne(ctx, bf)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return "", ErrDuplicateFiling
		}

		l.Error("AddFiling error", slog.String("error", err.Error()))
		return "", fmt.Errorf("error adding filing: %w", err)
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", ErrDecodeObjectId
	}
	return id.Hex(), nil
}

func (vr *vyparRepo) GetFiling(ctx context.Context, entityId uint64) (*stores.BusinessFilingMongo, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting logger from context: %w", err)
	}

	if entityId == 0 {
		return nil, ErrMissingEntityId
	}

	coll := vr.Store().Collection(FILING_COLLECTION)
	filter := bson.M{"entityId": entityId}

	var filing stores.BusinessFilingMongo
	err = coll.FindOne(ctx, filter).Decode(&filing)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("no filing found for entity ID %d: %w", entityId, ErrNoFilingFound)
		}
		l.Error("GetFiling error", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error getting filing: %w", err)
	}
	return &filing, nil
}

func (vr *vyparRepo) DeleteFiling(ctx context.Context, entityId uint64) (bool, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("error getting logger from context: %w", err)
	}

	if entityId == 0 {
		return false, ErrMissingEntityId
	}

	coll := vr.Store().Collection(FILING_COLLECTION)
	filter := bson.M{"entityId": entityId}
	result, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		l.Error("DeleteFiling error", slog.String("error", err.Error()))
		return false, fmt.Errorf("error deleting filing: %w", err)
	}
	return result.DeletedCount > 0, nil
}
