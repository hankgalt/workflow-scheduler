package scheduler

import (
	"context"

	"github.com/comfforts/errors"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cadence"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/mysqldb"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

const (
	ERR_MISSING_REQUIRED    = "error: missing required configuration"
	ERR_MISSING_CADENCE_CFG = "error: missing cadence configuration"
	ERR_DB_SETUP            = "error: data store setup"
	ERR_DB_RUN_INIT         = "error: data store workflow run init"
	ERR_DB_RUN_CLEANUP      = "error: data store workflow run cleanup"
	ERR_DB_PRINCIPALS_INIT  = "error: data store business principals init"
	ERR_DB_AGENTS_INIT      = "error: data store business agents init"
	ERR_DB_FILINGS_INIT     = "error: data store business filings init"
)

var (
	ErrMissingRequired   = errors.NewAppError(ERR_MISSING_REQUIRED)
	ErrMissingCadenceCfg = errors.NewAppError(ERR_MISSING_CADENCE_CFG)
)

type SchedulerService interface {
	// workflow run
	CreateRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error)
	UpdateRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error)
	GetRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error)
	DeleteRun(ctx context.Context, runId string) error
	SearchRuns(ctx context.Context, searchBy *models.RunParams) ([]*models.WorkflowRun, error)
	// entities
	AddAgent(ctx context.Context, ba *models.BusinessAgent) (*models.BusinessAgent, error)
	DeleteAgent(ctx context.Context, id string) error
	AddPrincipal(ctx context.Context, bp *models.BusinessPrincipal) (*models.BusinessPrincipal, error)
	DeletePrincipal(ctx context.Context, id string) error
	AddFiling(ctx context.Context, bf *models.BusinessFiling) (*models.BusinessFiling, error)
	DeleteFiling(ctx context.Context, id string) error
	// workflows
	ProcessFileSignalWorkflow(ctx context.Context, params *models.FileSignalParams) (*models.WorkflowRun, error)
	QueryWorkflowState(ctx context.Context, params *models.WorkflowQueryParams) (interface{}, error)
	Close() error
}

type serviceConfig struct {
	*mysqldb.DBConfig
	validateSetup bool
	cadenceCfg    string
}

// NewServiceConfig takes databaase credential
// returns service config instance or error
func NewServiceConfig(host, db, user, auth string, cadenceCfg string, validate bool) (*serviceConfig, error) {
	dbCfg, err := mysqldb.NewDBConfig(host, db, user, auth)
	if err != nil {
		return nil, err
	}

	return &serviceConfig{
		DBConfig:      dbCfg,
		validateSetup: validate,
		cadenceCfg:    cadenceCfg,
	}, nil
}

type schedulerService struct {
	db      *mysqldb.MYSQLDBClient
	cfg     *serviceConfig
	cadence *cadence.CadenceClient
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

	cc, err := cadence.NewCadenceClient(cfg.cadenceCfg, l)
	if err != nil {
		l.Error("error creating cadence client", zap.Error(err))
		return nil, err
	}
	bs.cadence = cc

	// err = bs.registerFileProcessingWorkflowGroup()
	// if err != nil {
	// 	logger.Error("error registering file processing workflow group", zap.Error(err))
	// 	return nil, err
	// }

	// err = bs.registerShopWorkflowGroup()
	// if err != nil {
	// 	logger.Error("error registering shop workflow group", zap.Error(err))
	// 	return nil, err
	// }

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

	ok = bs.db.Migrator().HasTable(models.BusinessAgent{})
	if !ok {
		err := bs.db.AutoMigrate(models.BusinessAgent{})
		if err != nil {
			bs.Error(ERR_DB_AGENTS_INIT, zap.Error(err))
			return errors.WrapError(err, ERR_DB_AGENTS_INIT)
		}
	}

	ok = bs.db.Migrator().HasTable(models.BusinessFiling{})
	if !ok {
		err := bs.db.AutoMigrate(models.BusinessFiling{})
		if err != nil {
			bs.Error(ERR_DB_FILINGS_INIT, zap.Error(err))
			return errors.WrapError(err, ERR_DB_FILINGS_INIT)
		}
	}

	ok = bs.db.Migrator().HasTable(models.BusinessPrincipal{})
	if !ok {
		err := bs.db.AutoMigrate(models.BusinessPrincipal{})
		if err != nil {
			bs.Error(ERR_DB_PRINCIPALS_INIT, zap.Error(err))
			return errors.WrapError(err, ERR_DB_PRINCIPALS_INIT)
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

// // registerFileProcessingWorkflowGroup registers file task group workflows and activities
// func (bs *schedulerService) registerFileProcessingWorkflowGroup() error {
// 	bs.cadence.RegisterWorkflow(file.FileProcessingWorkflow)
// 	bs.cadence.RegisterWorkflow(file.UploadFileWorkflowName)
// 	bs.cadence.RegisterWorkflow(file.DownloadFileWorkflowName)
// 	bs.cadence.RegisterActivityWithAlias(common.CreateRunActivity, common.CreateRunActivityName)
// 	bs.cadence.RegisterActivityWithAlias(common.UpdateRunActivity, common.UpdateRunActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.DownloadFileActivity, file.DownloadFileActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.DryRunActivity, file.DryRunActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.UploadFileActivity, file.UploadFileActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.CreateFileActivity, file.CreateFileActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.SaveFileActivity, file.SaveFileActivityName)
// 	bs.cadence.RegisterActivityWithAlias(file.DeleteFileActivity, file.DeleteFileActivityName)
// 	return nil
// }

// // registerShopWorkflowGroup registers shop task group workflows and activities
// func (bs *schedulerService) registerShopWorkflowGroup() error {
// 	bs.cadence.RegisterWorkflow(shwkfl.AddShopWorkflow)
// 	bs.cadence.RegisterWorkflow(shwkfl.ProcessShopDownloadWorkflow)
// 	bs.cadence.RegisterActivityWithAlias(common.CreateRunActivity, common.CreateRunActivityName)
// 	bs.cadence.RegisterActivityWithAlias(common.UpdateRunActivity, common.UpdateRunActivityName)
// 	bs.cadence.RegisterActivityWithAlias(shwkfl.AddGeoLocationActivity, shwkfl.AddGeoLocationActivityName)
// 	bs.cadence.RegisterActivityWithAlias(shwkfl.AddAddressActivity, shwkfl.AddAddressActivityName)
// 	bs.cadence.RegisterActivityWithAlias(shwkfl.AddShopActivity, shwkfl.AddShopActivityName)
// 	bs.cadence.RegisterActivityWithAlias(shwkfl.ProcessShopDownloadWorkflow, shwkfl.ProcessShopDownloadWorkflowName)
// 	return nil
// }
