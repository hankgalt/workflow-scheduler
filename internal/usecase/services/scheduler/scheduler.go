package scheduler

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	"github.com/hankgalt/workflow-scheduler/internal/repo/daud"
	"github.com/hankgalt/workflow-scheduler/internal/repo/vypar"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
)

type SchedulerService interface {
	ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (*stores.WorkflowRun, error)
	CreateRun(ctx context.Context, params *stores.WorkflowRun) (*stores.WorkflowRun, error)
	GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error)
	DeleteRun(ctx context.Context, runId string) error
	AddAgent(ctx context.Context, agent stores.Agent) (string, *stores.Agent, error)
	GetAgent(ctx context.Context, id string) (*stores.Agent, error)
	AddFiling(ctx context.Context, filing stores.Filing) (string, *stores.Filing, error)
	GetFiling(ctx context.Context, id string) (*stores.Filing, error)
	DeleteEntity(ctx context.Context, entityType stores.BusinessEntityType, id string) (bool, error)
	Close(ctx context.Context) error
}

type schedulerServiceConfig struct {
	TemporalConfig temporal.TemporalConfig
	MongoConfig    infra.StoreConfig
}

func NewSchedulerServiceConfig(temporalCfg temporal.TemporalConfig, mongoCfg infra.StoreConfig) schedulerServiceConfig {
	return schedulerServiceConfig{
		TemporalConfig: temporalCfg,
		MongoConfig:    mongoCfg,
	}
}

type schedulerService struct {
	temporal *temporal.TemporalClient
	daud     daud.DaudRepo
	vypar    vypar.VyparRepo
}

// NewSchedulerService initializes a new SchedulerService instance
func NewSchedulerService(ctx context.Context, cfg schedulerServiceConfig) (*schedulerService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:NewSchedulerService - error getting logger from context: %w", err)
	}

	// Create a new Temporal client
	tc, err := temporal.NewTemporalClient(ctx, cfg.TemporalConfig)
	if err != nil {
		l.Error("error creating temporal client", "error", err.Error())
		return nil, err
	}
	bs := &schedulerService{
		temporal: tc,
	}

	// Initialize MongoDB store for daud repository
	dms, err := mongostore.NewMongoStore(ctx, cfg.MongoConfig)
	if err != nil {
		l.Error("error getting MongoDB store", "error", err.Error())

		// Close the temporal client before returning
		tc.Close()
		return nil, err
	}

	// Initialize MongoDB store for vypar repository
	vms, err := mongostore.NewMongoStore(ctx, cfg.MongoConfig)
	if err != nil {
		l.Error("error getting MongoDB store for Vypar", "error", err.Error())

		// Close the temporal client before returning
		tc.Close()

		// Close the MongoDB store before returning
		if err := dms.Close(ctx); err != nil {
			l.Error("error closing MongoDB store", "error", err.Error())
		}

		return nil, err
	}

	// Initialize Daud repository
	dr, err := daud.NewDaudRepo(ctx, dms)
	if err != nil {
		l.Error("error creating Daud repository", "error", err.Error())
		// Close the temporal client before returning
		tc.Close()

		// Close the daud MongoDB store before returning
		if err := dms.Close(ctx); err != nil {
			l.Error("error closing daud `MongoDB store", "error", err.Error())
		}

		// Close the vypar MongoDB store before returning
		if err := vms.Close(ctx); err != nil {
			l.Error("error closing vypar MongoDB store", "error", err.Error())
		}

		return nil, err
	}
	bs.daud = dr

	// Initialize Vypar repository
	vr, err := vypar.NewVyparRepo(ctx, vms)
	if err != nil {
		l.Error("error creating Vypar repository", "error", err.Error())
		// Close the temporal client before returning
		tc.Close()

		// Close the daud MongoDB store before returning
		if err := dms.Close(ctx); err != nil {
			l.Error("error closing daud MongoDB store", "error", err.Error())
		}

		// Close the vypar MongoDB store before returning
		if err := vms.Close(ctx); err != nil {
			l.Error("error closing vypar MongoDB store", "error", err.Error())
		}

		return nil, err
	}
	bs.vypar = vr

	return bs, nil
}

// Workflows

func (bs *schedulerService) ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:ProcessLocalCSVToMongoWorkflow - error getting logger from context: %w", err)
	}

	runId, err := btchutils.GenerateRunID(req.Config)
	if err != nil {
		l.Error("error generating run ID", "error", err.Error())
		return nil, err
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("local-csv-mongo-%s", runId),
		TaskQueue:                btchwkfl.ApplicationName,
		WorkflowExecutionTimeout: time.Duration(24 * time.Hour),
		WorkflowTaskTimeout:      time.Minute * 5, // set to max, as there are decision tasks that'll take as long as max
		WorkflowIDReusePolicy:    1,
	}
	we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessLocalCSVMongo, &req)
	if err != nil {
		l.Error("error starting workflow", "error", err.Error())
		return nil, err
	}

	// TODO - add logic to create workflow run db record
	// if workflow starts successfully, there should be a record created in the database
	// handle failure, retries, etc.

	return &stores.WorkflowRun{
		RunId:      we.GetRunID(),
		WorkflowId: we.GetID(),
	}, nil
}

// Workflow Runs

func (bs *schedulerService) CreateRun(ctx context.Context, params *stores.WorkflowRun) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:CreateRun - error getting logger from context: %w", err)
	}

	runId, err := bs.daud.CreateRun(ctx, params)
	if err != nil {
		l.Error("error creating workflow run record", "error", err.Error(), "params", params)
		return nil, err
	}

	wkflRun, err := bs.daud.GetRunById(ctx, runId)
	if err != nil {
		l.Error("error getting workflow run by ID", "error", err.Error(), "runId", runId)
		return nil, err
	}

	return wkflRun, nil
}

func (bs *schedulerService) GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:GetRun - error getting logger from context: %w", err)
	}

	wkflRun, err := bs.daud.GetRun(ctx, runId)
	if err != nil {
		l.Error("error getting workflow run", "error", err.Error(), "runId", runId)
		return nil, err
	}

	return wkflRun, nil
}

func (bs *schedulerService) DeleteRun(ctx context.Context, runId string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("SchedulerService:DeleteRun - error getting logger from context: %w", err)
	}

	if err := bs.daud.DeleteRun(ctx, runId); err != nil {
		l.Error("error deleting workflow run", "error", err.Error(), "runId", runId)
		return err
	}

	return nil
}

// Business Entities

func (bs *schedulerService) AddAgent(ctx context.Context, agent stores.Agent) (string, *stores.Agent, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("SchedulerService:AddAgent - error getting logger from context: %w", err)
	}

	agId, err := bs.vypar.AddAgent(ctx, agent)
	if err != nil {
		l.Error("error adding agent", "error", err.Error(), "agent", agent)
		return "", nil, err
	}

	ag, err := bs.vypar.GetAgentById(ctx, agId)
	if err != nil {
		l.Error("error getting added agent for ID", "error", err.Error(), "id", agId)
		return "", nil, err
	}

	return agId, ag, nil
}

func (bs *schedulerService) GetAgent(ctx context.Context, id string) (*stores.Agent, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:GetAgent - error getting logger from context: %w", err)
	}

	agent, err := bs.vypar.GetAgentById(ctx, id)
	if err != nil {
		l.Error("error getting agent by ID", "error", err.Error(), "id", id)
		return nil, err
	}

	return agent, nil
}

func (bs *schedulerService) AddFiling(ctx context.Context, filing stores.Filing) (string, *stores.Filing, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("SchedulerService:AddFiling - error getting logger from context: %w", err)
	}
	fId, err := bs.vypar.AddFiling(ctx, filing)
	if err != nil {
		l.Error("error adding filing", "error", err.Error(), "filing", filing)
		return "", nil, err
	}
	fil, err := bs.vypar.GetFilingById(ctx, fId)
	if err != nil {
		l.Error("error getting added filing for ID", "error", err.Error(), "id", fId)
		return "", nil, err
	}
	return fId, fil, nil
}

func (bs *schedulerService) GetFiling(ctx context.Context, id string) (*stores.Filing, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:GetFiling - error getting logger from context: %w", err)
	}

	filing, err := bs.vypar.GetFilingById(ctx, id)
	if err != nil {
		l.Error("error getting filing by ID", "error", err.Error(), "id", id)
		return nil, err
	}

	return filing, nil
}

func (bs *schedulerService) DeleteEntity(ctx context.Context, entityType stores.BusinessEntityType, id string) (bool, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("SchedulerService:DeleteEntity - error getting logger from context: %w", err)
	}

	switch entityType {
	case stores.EntityTypeAgent:
		return bs.vypar.DeleteAgentById(ctx, id)
	case stores.EntityTypeFiling:
		return bs.vypar.DeleteFilingById(ctx, id)
	default:
		l.Error("unsupported entity type for deletion", "entityType", entityType)
		return false, fmt.Errorf("unsupported entity type for deletion: %s", entityType)
	}
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
