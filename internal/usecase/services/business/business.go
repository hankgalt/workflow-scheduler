package business

import (
	"context"
	"errors"
	"fmt"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/repo/vypar"
	"go.mongodb.org/mongo-driver/mongo"
)

type BusinessService interface {
	AddAgent(ctx context.Context, agent stores.Agent) (string, *stores.Agent, error)
	GetAgent(ctx context.Context, id string) (*stores.Agent, error)
	AddFiling(ctx context.Context, filing stores.Filing) (string, *stores.Filing, error)
	GetFiling(ctx context.Context, id string) (*stores.Filing, error)
	DeleteEntity(ctx context.Context, entityType stores.BusinessEntityType, id string) (bool, error)
	Close(ctx context.Context) error
}

type businessServiceConfig struct {
	MongoConfig infra.StoreConfig
}

func NewBusinessServiceConfig(mongoCfg infra.StoreConfig) businessServiceConfig {
	return businessServiceConfig{
		MongoConfig: mongoCfg,
	}
}

type businessService struct {
	mongoClient *mongo.Client
	vypar       vypar.VyparRepo
}

// NewBusinessService initializes a new BusinessService instance
func NewBusinessService(ctx context.Context, cfg businessServiceConfig) (*businessService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	// Initialize MongoDB store for daud repository
	ms, err := mongostore.NewMongoStore(ctx, cfg.MongoConfig)
	if err != nil {
		l.Error("error getting Mongo store", "error", err.Error())
		return nil, err
	}

	// Initialize Vypar repository
	vr, err := vypar.NewVyparRepo(ctx, ms)
	if err != nil {
		l.Error("error creating vypar repository", "error", err.Error())

		// Close the Mongo store before returning
		if dErr := ms.Close(ctx); dErr != nil {
			l.Error("error closing Mongo store", "error", dErr.Error())
			err = errors.Join(err, dErr)
		}

		return nil, err
	}

	return &businessService{
		vypar: vr,
	}, nil
}

// Business Entities

func (bs *businessService) AddAgent(ctx context.Context, agent stores.Agent) (string, *stores.Agent, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
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

func (bs *businessService) GetAgent(ctx context.Context, id string) (*stores.Agent, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	agent, err := bs.vypar.GetAgentById(ctx, id)
	if err != nil {
		l.Error("error getting agent by ID", "error", err.Error(), "id", id)
		return nil, err
	}

	return agent, nil
}

func (bs *businessService) AddFiling(ctx context.Context, filing stores.Filing) (string, *stores.Filing, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
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

func (bs *businessService) GetFiling(ctx context.Context, id string) (*stores.Filing, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	filing, err := bs.vypar.GetFilingById(ctx, id)
	if err != nil {
		l.Error("error getting filing by ID", "error", err.Error(), "id", id)
		return nil, err
	}

	return filing, nil
}

func (bs *businessService) DeleteEntity(ctx context.Context, entityType stores.BusinessEntityType, id string) (bool, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
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

// Close closes business service connections
func (bs *businessService) Close(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := bs.vypar.Close(ctx); err != nil {
		l.Error("error closing vypar repository connection", "error", err.Error())
		return err
	}
	return nil
}
