package scheduler

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/mysqldb"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

const TEST_DIR = "data"
const CADENCE_CONFIG = "../../../cmd/scheduler/config/development.yaml"

func TestSchedulerService(t *testing.T) {
	setupEnv(t)
	testServiceConfig(t)

	l := logger.NewTestAppZapLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		bs *schedulerService,
	){
		"database setup, succeeds": testDatabaseSetup,
	} {
		t.Run(scenario, func(t *testing.T) {
			bs, teardown := setup(t, l)
			defer teardown()
			fn(t, bs)
		})
	}

	err := os.RemoveAll(TEST_DIR)
	require.NoError(t, err)
}

func setupEnv(t *testing.T) {
	t.Helper()

	cPath := os.Getenv("CERTS_PATH")
	if cPath != "" && !strings.Contains(cPath, "../../../") {
		cPath = fmt.Sprintf("../%s", cPath)
	}
	pPath := os.Getenv("POLICY_PATH")
	if pPath != "" && !strings.Contains(pPath, "../../../") {
		pPath = fmt.Sprintf("../%s", pPath)
	}
	crPath := os.Getenv("CREDS_PATH")
	if crPath != "" && !strings.Contains(crPath, "../../../") {
		crPath = fmt.Sprintf("../%s", crPath)
	}

	os.Setenv("CERTS_PATH", cPath)
	os.Setenv("POLICY_PATH", pPath)
	os.Setenv("CREDS_PATH", crPath)
}

func setup(t *testing.T, l *zap.Logger) (
	bs *schedulerService,
	teardown func(),
) {
	t.Helper()

	serviceCfg, err := NewServiceConfig("localhost", "", "", "", CADENCE_CONFIG, false)
	require.NoError(t, err)

	bs, err = NewSchedulerService(serviceCfg, l)
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
	srvCfg, err := NewServiceConfig(host, "", "", "", CADENCE_CONFIG, false)
	require.NoError(t, err)
	require.Equal(t, srvCfg.DBConfig.Host, host)

	_, err = NewServiceConfig("", "", "", "", CADENCE_CONFIG, false)
	require.Error(t, err)
	require.Equal(t, err, mysqldb.ErrMissingRequired)
}

func testDatabaseSetup(t *testing.T, ss *schedulerService) {
	t.Helper()

	testWorkflowRunMigration(t, ss)
	testAgentsMigration(t, ss)
	testPrincipalsMigration(t, ss)
	testFilingsMigration(t, ss)
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

func testAgentsMigration(t *testing.T, ss *schedulerService) {
	t.Helper()

	ok := ss.db.Migrator().HasTable(models.BusinessAgent{})
	if ok {
		err := ss.db.Migrator().DropTable(models.BusinessAgent{})
		require.NoError(t, err)
	}

	err := ss.db.AutoMigrate(models.BusinessAgent{})
	require.NoError(t, err)
}

func testPrincipalsMigration(t *testing.T, ss *schedulerService) {
	t.Helper()

	ok := ss.db.Migrator().HasTable(models.BusinessPrincipal{})
	if ok {
		err := ss.db.Migrator().DropTable(models.BusinessPrincipal{})
		require.NoError(t, err)
	}

	err := ss.db.AutoMigrate(models.BusinessPrincipal{})
	require.NoError(t, err)
}

func testFilingsMigration(t *testing.T, ss *schedulerService) {
	t.Helper()

	ok := ss.db.Migrator().HasTable(models.BusinessFiling{})
	if ok {
		err := ss.db.Migrator().DropTable(models.BusinessFiling{})
		require.NoError(t, err)
	}

	err := ss.db.AutoMigrate(models.BusinessFiling{})
	require.NoError(t, err)
}
