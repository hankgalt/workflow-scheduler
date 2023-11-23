package scheduler

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/mysqldb"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

const TEST_DIR = "data"

func TestSchedulerService(t *testing.T) {
	testServiceConfig(t)

	logger := logger.NewTestAppZapLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		bs *schedulerService,
	){
		"database setup, succeeds": testDatabaseSetup,
	} {
		t.Run(scenario, func(t *testing.T) {
			bs, teardown := setup(t, logger)
			defer teardown()
			fn(t, bs)
		})
	}

	err := os.RemoveAll(TEST_DIR)
	require.NoError(t, err)
}

func setup(t *testing.T, logger *zap.Logger) (
	bs *schedulerService,
	teardown func(),
) {
	t.Helper()

	serviceCfg, err := NewServiceConfig("localhost", "", "", "", false)
	require.NoError(t, err)

	bs, err = NewSchedulerService(serviceCfg, logger)
	require.NoError(t, err)

	return bs, func() {
		t.Logf(" %s ended, will clean up resources", t.Name())
		err = bs.Close()
		require.NoError(t, err)

		err = os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testServiceConfig(t *testing.T) {
	host := "localhost"
	srvCfg, err := NewServiceConfig(host, "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, srvCfg.DBConfig.Host, host)

	_, err = NewServiceConfig("", "", "", "", false)
	require.Error(t, err)
	require.Equal(t, err, mysqldb.ErrMissingRequired)
}

func testDatabaseSetup(t *testing.T, ss *schedulerService) {
	t.Helper()

	testWorkflowRunMigration(t, ss)
}

func testWorkflowRunMigration(t *testing.T, ss *schedulerService) {
	t.Helper()

	ok := ss.db.Migrator().HasTable(models.WorkflowRun{})
	if ok {
		err := ss.db.Migrator().DropTable(models.WorkflowRun{})
		require.NoError(t, err)
	}

	err := ss.db.AutoMigrate(models.WorkflowRun{})
	require.NoError(t, err)
}
