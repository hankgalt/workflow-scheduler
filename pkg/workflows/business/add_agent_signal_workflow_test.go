package business_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

type EntityWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestEntityWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(EntityWorkflowTestSuite))
}

func (s *EntityWorkflowTestSuite) SetupTest() {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(bizwkfl.AddAgentSignalWorkflow)

	s.env.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{
		Name: comwkfl.CreateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(comwkfl.UpdateRunActivity, activity.RegisterOptions{
		Name: comwkfl.UpdateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(comwkfl.SearchRunActivity, activity.RegisterOptions{
		Name: comwkfl.SearchRunActivityName,
	})
	s.env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{
		Name: fiwkfl.DownloadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddAgentActivity, activity.RegisterOptions{
		Name: bizwkfl.AddAgentActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{
		Name: bizwkfl.AddPrincipalActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddFilingActivity, activity.RegisterOptions{
		Name: bizwkfl.AddFilingActivityName,
	})

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

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_BUCKET)
		return
	}

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("error initializing scheduler grpc client", zap.Error(err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	ctx = context.WithValue(ctx, cloud.CloudClientContextKey, cloudClient)
	ctx = context.WithValue(ctx, cloud.CloudBucketContextKey, bucket)
	ctx = context.WithValue(ctx, fiwkfl.DataPathContextKey, TEST_DIR)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(comwkfl.ONE_DAY)
}

func (s *EntityWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *EntityWorkflowTestSuite) Test_AddAgentSignalWorkflow() {
	list := createFieldValueList()
	signalAgent := func(res *models.CSVRecord) func() {
		return func() {
			s.env.SignalWorkflow(bizwkfl.AGENT_SIG_CHAN, res)
		}
	}
	for i, v := range list {
		fmt.Printf("signalling result num %d: %v\n", i, v)
		s.env.RegisterDelayedCallback(signalAgent(&v), 0)
	}

	s.env.ExecuteWorkflow(bizwkfl.AddAgentSignalWorkflow, &models.BatchInfo{
		BatchCount: len(list),
	})
	s.True(s.env.IsWorkflowCompleted())
	var batchInfo models.BatchInfo
	err := s.env.GetWorkflowResult(&batchInfo)

	s.NoError(err)
	s.Equal(0, batchInfo.ErrCount)
	s.Equal(3, batchInfo.BatchCount)
	s.Equal(3, batchInfo.ProcessedCount)
}

func createFieldValueList() []models.CSVRecord {
	items := []models.CSVRecord{
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Plaza Hollywood",
			"country":   "CN",
			"longitude": "114.20169067382812",
			"latitude":  "22.340700149536133",
			"storeId":   "1",
		},
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Exchange Square",
			"country":   "CN",
			"longitude": "114.15818786621094",
			"latitude":  "22.283939361572266",
			"storeId":   "6",
		},
		{
			"city":      "Kowloon",
			"org":       "starbucks",
			"name":      "Telford Plaza",
			"country":   "CN",
			"longitude": "114.21343994140625",
			"latitude":  "22.3228702545166",
			"storeId":   "8",
		},
	}
	return items
}
