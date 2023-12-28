package file_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"

	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

const TEST_DIR = "data"

type FileWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment

	cloudClient cloud.Client
}

func TestFileWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(FileWorkflowTestSuite))
}

func (s *FileWorkflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(fiwkfl.UploadFileWorkflow)
	s.env.RegisterWorkflow(fiwkfl.DownloadFileWorkflow)
	s.env.RegisterWorkflow(fiwkfl.DeleteFileWorkflow)

	s.env.RegisterActivityWithOptions(common.CreateRunActivity, activity.RegisterOptions{
		Name: common.CreateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(common.UpdateRunActivity, activity.RegisterOptions{
		Name: common.UpdateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(fiwkfl.UploadFileActivity, activity.RegisterOptions{
		Name: fiwkfl.UploadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{
		Name: fiwkfl.DownloadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(fiwkfl.DeleteFileActivity, activity.RegisterOptions{
		Name: fiwkfl.DeleteFileActivityName,
	})

	l := logger.NewTestAppZapLogger(TEST_DIR)
	cloudCfg, err := cloud.NewCloudConfig("", TEST_DIR)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CFG_INIT, zap.Error(err))
		panic(err)
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, l)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CLIENT_INIT, zap.Error(err))
		panic(err)
	}
	s.cloudClient = cloudClient

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_BUCKET)
		panic(err)
	}

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(common.ERR_SCH_CLIENT_INIT, zap.Error(err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	ctx = context.WithValue(ctx, cloud.CloudClientContextKey, cloudClient)
	ctx = context.WithValue(ctx, cloud.CloudBucketContextKey, bucket)
	ctx = context.WithValue(ctx, fiwkfl.DataPathContextKey, TEST_DIR)
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})
}

func (s *FileWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	err := s.cloudClient.Close()
	s.NoError(err)

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}
