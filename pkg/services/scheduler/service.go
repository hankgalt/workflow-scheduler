package scheduler

import (
	"context"

	"github.com/comfforts/errors"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/mysqldb"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

const (
	ERR_MISSING_REQUIRED    = "error: missing required configuration"
	ERR_MISSING_CADENCE_CFG = "error: missing cadence configuration"
	ERR_DB_SETUP            = "error: data store setup"
	ERR_DB_RUN_INIT         = "error: data store workflow run init"
	ERR_DB_RUN_CLEANUP      = "error: data store workflow run cleanup"
)

var (
	ErrMissingRequired = errors.NewAppError(ERR_MISSING_REQUIRED)
)

type SchedulerService interface {
	// workflow run
	CreateRun(ctx context.Context, params *models.RunUpdateParams) (*models.WorkflowRun, error)
	UpdateRun(ctx context.Context, params *models.RunUpdateParams) (*models.WorkflowRun, error)
	GetRun(ctx context.Context, params *models.RunUpdateParams) (*models.WorkflowRun, error)
	DeleteRun(ctx context.Context, runId string) error
	Close() error
}

type serviceConfig struct {
	*mysqldb.DBConfig
	validateSetup bool
}

// NewServiceConfig takes databaase credential
// returns service config instance or error
func NewServiceConfig(host, db, user, auth string, validate bool) (*serviceConfig, error) {
	dbCfg, err := mysqldb.NewDBConfig(host, db, user, auth)
	if err != nil {
		return nil, err
	}

	return &serviceConfig{
		DBConfig:      dbCfg,
		validateSetup: validate,
	}, nil
}

type schedulerService struct {
	db  *mysqldb.MYSQLDBClient
	cfg *serviceConfig
	*zap.Logger
}

// NewSchedulerService takes service config & logger
// sets up database client with provided DB config & validates database setup
// returns scheduler service instance or error
func NewSchedulerService(cfg *serviceConfig, l *zap.Logger) (*schedulerService, error) {
	db, err := mysqldb.NewMYSQLClient(cfg.DBConfig, l)
	if err != nil {
		l.Error("error creating mysql db client", zap.Error(err))
		return nil, err
	}

	bs := &schedulerService{
		Logger: l,
		cfg:    cfg,
		db:     db,
	}

	if cfg.validateSetup {
		err = bs.setupDatabase()
		if err != nil {
			l.Error(ERR_DB_SETUP, zap.Error(err))
			return nil, errors.WrapError(err, ERR_DB_SETUP)
		}
	}

	return bs, nil
}

// Close closes scheduler database connections
func (bs *schedulerService) Close() error {
	err := bs.db.Close()
	if err != nil {
		bs.Error("error closing scheduler service", zap.Error(err))
		return err
	}
	return nil
}

// ResetDatabase resets scheduler data tables
func (ss *schedulerService) ResetDatabase() error {
	err := ss.resetWorkflowRun()
	if err != nil {
		return err
	}

	return nil
}

// setupDatabase validates scheduler data tables
func (bs *schedulerService) setupDatabase() error {
	ok := bs.db.Migrator().HasTable(models.WorkflowRun{})
	if !ok {
		err := bs.db.AutoMigrate(models.WorkflowRun{})
		if err != nil {
			bs.Error(ERR_DB_RUN_INIT, zap.Error(err))
			return errors.WrapError(err, ERR_DB_RUN_INIT)
		}
	}

	return nil
}

func (ss *schedulerService) resetWorkflowRun() error {
	ok := ss.db.Migrator().HasTable(models.WorkflowRun{})
	if ok {
		err := ss.db.Migrator().DropTable(models.WorkflowRun{})
		if err != nil {
			ss.Error(ERR_DB_RUN_CLEANUP, zap.Error(err))
			return errors.WrapError(err, ERR_DB_RUN_CLEANUP)
		}
	}
	return nil
}
