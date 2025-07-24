package common_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const TEST_DIR = "data"

const (
	ActivityAlias          string = "some-random-activity-alias"
	CreateRunActivityAlias string = "create-run-activity-alias"
	UpdateRunActivityAlias string = "update-run-activity-alias"
	SearchRunActivityAlias string = "search-run-activity-alias"
)

type CommonActivitiesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestCommonWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(CommonActivitiesTestSuite))
}

func (s *CommonActivitiesTestSuite) Test_LocalActivity() {
	localActivityFn := func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}

	env := s.NewTestActivityEnvironment()
	result, err := env.ExecuteLocalActivity(localActivityFn, "local_activity")
	s.NoError(err)
	var laResult string
	err = result.Get(&laResult)
	s.NoError(err)
	fmt.Println("laResult: ", laResult)
	s.Equal("hello local_activity", laResult)
}

func (s *CommonActivitiesTestSuite) Test_ActivityRegistration() {
	activityFn := func(msg string) (string, error) {
		return msg, nil
	}

	env := s.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: ActivityAlias})
	input := "some random input"

	encodedValue, err := env.ExecuteActivity(activityFn, input)
	s.NoError(err)
	output := ""
	err = encodedValue.Get(&output)
	s.NoError(err)
	s.Equal(input, output)

	encodedValue, err = env.ExecuteActivity(ActivityAlias, input)
	s.NoError(err)
	output = ""
	err = encodedValue.Get(&output)
	s.NoError(err)
	fmt.Println("output: ", output)
	s.Equal(input, output)
}

func (s *CommonActivitiesTestSuite) Test_CreateRunActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register CreateRunActivity
	env.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{Name: CreateRunActivityAlias})

	// create scheduler client
	cl := logger.NewTestAppZapLogger(TEST_DIR)
	schClient, err := scheduler.NewClient(cl, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(comwkfl.ERR_SCH_CLIENT_INIT, slog.String("error", err.Error()))
		panic(err)
	}

	defer func() {
		err := schClient.Close()
		s.NoError(err)
	}()

	// set scheduler client in test environment context
	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	reqster := "test-create-run@test.com"
	wkflId := "C3r43r-T3s73k7l0w"
	runId := "S3r43r-T3s7Ru41d"
	defer func() {
		if runId != "" {
			err := s.deleteRun(schClient, runId)
			s.NoError(err)
		}
	}()

	req := &models.RunParams{
		WorkflowId:  wkflId,
		RunId:       runId,
		RequestedBy: reqster,
	}

	// test create run activity
	res, err := s.testCreateRun(env, req)
	s.NoError(err)
	s.Equal(req.RunId, res.RunId)
	s.Equal(req.WorkflowId, res.WorkflowId)
	s.Equal(req.RequestedBy, res.RequestedBy)
	s.Equal(string(models.STARTED), res.Status)
}

func (s *CommonActivitiesTestSuite) Test_CreateRunActivity_Error() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register CreateRunActivity
	env.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{Name: CreateRunActivityAlias})

	// create scheduler client
	cl := logger.NewTestAppZapLogger(TEST_DIR)
	schClient, err := scheduler.NewClient(cl, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(comwkfl.ERR_SCH_CLIENT_INIT, slog.String("error", err.Error()))
		panic(err)
	}

	defer func() {
		err := schClient.Close()
		s.NoError(err)
	}()

	// set scheduler client in test environment context
	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	reqster := "test-create-run@test.com"
	wkflId := ""
	runId := "S3r43r-T3s7Ru41d"

	req := &models.RunParams{
		WorkflowId:  wkflId,
		RunId:       runId,
		RequestedBy: reqster,
	}

	// test create run activity
	_, err = s.testCreateRun(env, req)
	s.Error(err)
	cusErr, ok := err.(*temporal.ActivityError)
	s.Equal(true, ok)
	cause := cusErr.Unwrap()
	appErr, ok := cause.(*temporal.ApplicationError)
	s.Equal(true, ok)
	s.Equal(comwkfl.ERR_CREATING_RUN, appErr.Type())
}

func (s *CommonActivitiesTestSuite) Test_UpdateRunActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register UpdateRunActivity
	env.RegisterActivityWithOptions(comwkfl.UpdateRunActivity, activity.RegisterOptions{Name: UpdateRunActivityAlias})

	// create scheduler client
	cl := logger.NewTestAppZapLogger(TEST_DIR)
	schClient, err := scheduler.NewClient(cl, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(comwkfl.ERR_SCH_CLIENT_INIT, slog.String("error", err.Error()))
		panic(err)
	}
	defer func() {
		err := schClient.Close()
		s.NoError(err)
	}()

	// set scheduler client in test environment context
	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// create a workflow run
	requester := "test-create-run@test.com"
	wkflId := "C3r43r-T3s73k7l0w"
	runId := "C3r43r-T3s7Ru41d"
	req := &api.RunRequest{
		WorkflowId:  wkflId,
		RunId:       runId,
		RequestedBy: requester,
	}
	wkflRun, err := s.createRun(schClient, req)
	s.NoError(err)

	defer func() {
		err := s.deleteRun(schClient, runId)
		s.NoError(err)
	}()

	// test update run activity
	runId = s.testUpdateRun(env, wkflRun)

}

func (s *CommonActivitiesTestSuite) Test_SearchRunActivity() {

	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register SearchRunActivity
	env.RegisterActivityWithOptions(comwkfl.SearchRunActivity, activity.RegisterOptions{Name: SearchRunActivityAlias})

	// create scheduler client
	cl := logger.NewTestAppZapLogger(TEST_DIR)
	schClient, err := scheduler.NewClient(cl, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(comwkfl.ERR_SCH_CLIENT_INIT, slog.String("error", err.Error()))
		panic(err)
	}

	defer func() {
		err := schClient.Close()
		s.NoError(err)
	}()

	// set scheduler client in test environment context
	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// create a workflow run
	requester := "test-search-run@test.com"
	wkflId1 := "C3r43r-T3s73k7l0w"
	runId1 := "C3r43r-T3s7Ru41d"
	wkflType := "fileSignalWorkflow"
	req1 := &api.RunRequest{
		WorkflowId:  wkflId1,
		RunId:       runId1,
		RequestedBy: requester,
		Type:        wkflType,
	}
	wkflRun1, err := s.createRun(schClient, req1)
	s.NoError(err)

	defer func() {
		err := s.deleteRun(schClient, wkflRun1.RunId)
		s.NoError(err)
	}()

	// create a workflow run
	wkflId2 := "C3r43r-T3s73k7l0w2"
	runId2 := "C3r43r-T3s7Ru41d2"
	req2 := &api.RunRequest{
		WorkflowId:  wkflId2,
		RunId:       runId2,
		RequestedBy: requester,
		Type:        wkflType,
	}
	wkflRun2, err := s.createRun(schClient, req2)
	s.NoError(err)

	defer func() {
		err := s.deleteRun(schClient, wkflRun2.RunId)
		s.NoError(err)
	}()

	sResp, err := schClient.SearchRuns(ctx, &api.SearchRunRequest{
		Type: wkflType,
	})
	s.NoError(err)
	s.Equal(sResp.Ok, true)
	s.Equal(len(sResp.Runs), 2)
}

func (s *CommonActivitiesTestSuite) testCreateRun(env *testsuite.TestActivityEnvironment, req *models.RunParams) (*api.WorkflowRun, error) {
	l := s.GetLogger()

	resp, err := env.ExecuteActivity(comwkfl.CreateRunActivity, req)
	if err != nil {
		return nil, err
	}

	var result api.WorkflowRun
	err = resp.Get(&result)
	if err != nil {
		return nil, err
	}
	l.Info("create run activity response", slog.String("runId", result.RunId), slog.String("wkflId", result.WorkflowId), slog.String("status", result.Status))
	return &result, nil
}

func (s *CommonActivitiesTestSuite) testUpdateRun(env *testsuite.TestActivityEnvironment, wkflRun *api.WorkflowRun) string {
	l := s.GetLogger()

	reqster := "test-update-run@test.com"
	req := &models.RunParams{
		WorkflowId:  wkflRun.WorkflowId,
		RunId:       wkflRun.RunId,
		RequestedBy: reqster,
		Status:      string(models.UPLOADED),
	}

	resp, err := env.ExecuteActivity(comwkfl.UpdateRunActivity, req)
	s.NoError(err)

	var result api.WorkflowRun
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(result.RunId, wkflRun.RunId)
	s.Equal(result.WorkflowId, wkflRun.WorkflowId)
	s.Equal(result.Status, string(models.UPLOADED))
	l.Info("update run activity response", slog.String("runId", result.RunId), slog.String("wkflId", result.WorkflowId), slog.String("status", result.Status))
	return wkflRun.RunId
}

func (s *CommonActivitiesTestSuite) createRun(cl scheduler.Client, req *api.RunRequest) (*api.WorkflowRun, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if wfRun, err := cl.CreateRun(ctx, req); err != nil {
		return nil, err
	} else {
		return wfRun.Run, nil
	}
}

func (s *CommonActivitiesTestSuite) deleteRun(cl scheduler.Client, runId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cl.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: runId,
	}); err != nil {
		return err
	}
	return nil
}

func (s *CommonActivitiesTestSuite) getTestLogger() *slog.Logger {
	// create log level var
	logLevel := &slog.LevelVar{}
	// set log level
	logLevel.Set(slog.LevelDebug)
	// create log handler options
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}
	// create log handler
	handler := slog.NewJSONHandler(os.Stdout, opts)
	// create logger
	l := slog.New(handler)
	// set default logger
	slog.SetDefault(l)

	return l
}
