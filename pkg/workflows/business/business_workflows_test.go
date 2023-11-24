package business_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
)

const TEST_DIR = "data"

type BusinessWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *BusinessWorkflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(bizwkfl.AddAgentSignalWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ProcessCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVRecordsWorkflow)

	s.env.RegisterActivityWithOptions(bizwkfl.AddAgentActivity, activity.RegisterOptions{
		Name: bizwkfl.AddAgentActivityName,
	})

	s.env.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{
		Name: bizwkfl.AddPrincipalActivityName,
	})

	s.env.RegisterActivityWithOptions(bizwkfl.AddFilingActivity, activity.RegisterOptions{
		Name: bizwkfl.AddFilingActivityName,
	})

	s.env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{
		Name: bizwkfl.GetCSVHeadersActivityName,
	})

	s.env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{
		Name: bizwkfl.GetCSVOffsetsActivityName,
	})

	s.env.RegisterActivityWithOptions(bizwkfl.ReadCSVActivity, activity.RegisterOptions{
		Name: bizwkfl.ReadCSVActivityName,
	})

	l := logger.NewTestAppZapLogger(TEST_DIR)

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("error initializing business grpc client", zap.Error(err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(common.ONE_DAY)
}

func (s *BusinessWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(BusinessWorkflowTestSuite))
}

func (s *BusinessWorkflowTestSuite) Test_AddAgentSignalWorkflow() {
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
