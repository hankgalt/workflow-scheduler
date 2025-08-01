package scheduler

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	"github.com/hankgalt/workflow-scheduler/internal/repo/daud"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

type SchedulerService interface {
	ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (stores.WorkflowRun, error)
	Close(ctx context.Context) error
}

type schedulerService struct {
	temporal *temporal.TemporalClient
	daud     daud.DaudRepo
}

// NewSchedulerService initializes a new SchedulerService instance
func NewSchedulerService(ctx context.Context) (*schedulerService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:NewSchedulerService - error getting logger from context: %w", err)
	}

	tc, err := temporal.NewTemporalClient(ctx)
	if err != nil {
		l.Error("error creating temporal client", "error", err.Error())
		return nil, err
	}
	bs := &schedulerService{
		temporal: tc,
	}

	// Get MongoDB configuration
	nmCfg := mongostore.GetMongoConfig()
	ms, err := mongostore.GetMongoStore(ctx, nmCfg)
	if err != nil {
		l.Error("error getting MongoDB store", "error", err.Error())

		// Close the temporal client before returning
		tc.Close()
		return nil, err
	}

	// Initialize Daud repository
	dr, err := daud.NewDaudRepo(ms)
	if err != nil {
		l.Error("error creating Daud repository", "error", err.Error())
		// Close the MongoDB store before returning
		if err := ms.Close(ctx); err != nil {
			l.Error("error closing MongoDB store", "error", err.Error())
		}
		tc.Close()
		return nil, err
	}
	bs.daud = dr

	return bs, nil
}

func (bs *schedulerService) ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return stores.WorkflowRun{}, fmt.Errorf("SchedulerService:ProcessLocalCSVToMongoWorkflow - error getting logger from context: %w", err)
	}

	runId, err := btchutils.GenerateRunID(req.Config)
	if err != nil {
		l.Error("error generating run ID", "error", err.Error())
		return stores.WorkflowRun{}, err
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("local-csv-mongo-%s", runId),
		TaskQueue:                btchwkfl.ApplicationName,
		WorkflowExecutionTimeout: time.Duration(24 * time.Hour),
		WorkflowTaskTimeout:      time.Minute * 5, // set to max, as there are decision tasks that'll take as long as max
		WorkflowIDReusePolicy:    1,
	}
	we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessLocalCSVMongoWorkflow, req)
	if err != nil {
		l.Error("error starting workflow", "error", err.Error())
		return stores.WorkflowRun{}, err
	}

	// TODO - add logic to create workflow run db record
	// if workflow starts successfully, there should be a record created in the database
	// handle failure, retries, etc.

	return stores.WorkflowRun{
		RunId:      we.GetRunID(),
		WorkflowId: we.GetID(),
	}, nil
}

// Close closes scheduler upstream client connections
func (bs *schedulerService) Close(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("SchedulerService:Close - error getting logger from context: %w", err)
	}

	bs.temporal.Close()

	if err := bs.daud.Close(ctx); err != nil {
		l.Error("error closing Daud repository", "error", err.Error())
		return err
	}
	return nil
}
