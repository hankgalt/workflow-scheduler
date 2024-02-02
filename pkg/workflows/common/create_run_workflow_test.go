package common_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

type CommonWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(CommonWorkflowTestSuite))
}

func (s *CommonWorkflowTestSuite) Test_CreateRunWorkflow() {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	requester := "test-create-run@test.com"
	wkflId := "C3r43r-T3s73k7l0w"
	runId := "C3r43r-T3s7Ru41d"
	req := &models.RunParams{
		WorkflowId:  wkflId,
		RunId:       runId,
		RequestedBy: requester,
	}
	expectedCall := []string{
		comwkfl.CreateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create run
			var input models.RunParams
			s.NoError(args.Get(&input))
			s.Equal(requester, input.RequestedBy)
		default:
			panic("Test_CreateRunWorkflow unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_CreateRunWorkflow panicked", zap.Any("error", err), zap.String("wkfl", common.CreateRunWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_CreateRunWorkflow error", zap.Any("error", err))
		}

		l.Info("cleaning up", zap.String("wkfl", common.CreateRunWorkflowName))

		sOpts := scheduler.NewDefaultClientOption()
		sOpts.Caller = "CommonWorkflowTestSuite"
		schClient, err := scheduler.NewClient(l, sOpts)
		s.NoError(err)

		if err = deleteRun(schClient, runId); err != nil {
			l.Error("Test_CreateRunWorkflow error deleting run", zap.Error(err), zap.String("wkfl", common.CreateRunWorkflowName))
		}

		if err = schClient.Close(); err != nil {
			l.Error("Test_CreateRunWorkflow error closing scheduler client", zap.Error(err), zap.String("wkfl", common.CreateRunWorkflowName))
		}
	}()

	s.env.ExecuteWorkflow(comwkfl.CreateRunWorkflow, req)
	s.True(s.env.IsWorkflowCompleted())
	var resp models.RunParams
	err := s.env.GetWorkflowResult(&resp)

	s.NoError(err)
	s.Equal(runId, resp.RunId)
	s.Equal(wkflId, resp.WorkflowId)
	s.Equal(requester, resp.RequestedBy)
}

func (s *CommonWorkflowTestSuite) SetupTest() {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(comwkfl.CreateRunWorkflow)

	s.env.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{
		Name: comwkfl.CreateRunActivityName,
	})

	ctx := context.Background()
	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("error initializing scheduler grpc client", zap.Error(err))
		panic(err)
	}
	ctx = context.WithValue(ctx, scheduler.SchedulerClientContextKey, schClient)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(comwkfl.ONE_DAY)
}

func (s *CommonWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func deleteRun(cl scheduler.Client, runId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cl.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: runId,
	}); err != nil {
		return err
	}
	return nil
}
