package business_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
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
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common"
)

const TEST_DIR = "data"
const DATA_PATH string = "scheduler"
const MISSING_FILE_NAME string = "geo.csv"
const LIVE_FILE_NAME string = "Agents-sm.csv"

const (
	ActivityAlias             string = "some-random-activity-alias"
	AddAgentActivityAlias     string = "add-agent-activity-alias"
	AddPrincipalActivityAlias string = "add-principal-activity-alias"
	AddFilingActivityAlias    string = "add-filing-activity-alias"
	CSVHeadersActivityAlias   string = "get-csv-headers-activity-alias"
	CSVOffsetsActivityAlias   string = "get-csv-offsets-activity-alias"
	ReadCSVActivityAlias      string = "read-csv-activity-alias"
)

type BusinessActivitiesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestBusinessActivitiesTestSuite(t *testing.T) {
	suite.Run(t, new(BusinessActivitiesTestSuite))
}

func (s *BusinessActivitiesTestSuite) Test_LocalActivity() {
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

func (s *BusinessActivitiesTestSuite) Test_ActivityRegistration() {
	activityFn := func(msg string) (string, error) {
		return msg, nil
	}

	env := s.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: ActivityAlias})
	input := "some random input"

	encodedValue, err := env.ExecuteActivity(activityFn, input)
	s.NoError(err)
	output := ""
	encodedValue.Get(&output)
	s.Equal(input, output)

	encodedValue, err = env.ExecuteActivity(ActivityAlias, input)
	s.NoError(err)
	output = ""
	encodedValue.Get(&output)
	fmt.Println("output: ", output)
	s.Equal(input, output)
}

func (s *BusinessActivitiesTestSuite) Test_AddPrincipalActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register AddPrincipalActivity
	env.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{Name: AddPrincipalActivityAlias})

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

	// build id & entityId
	id := "535342788"
	num, err := strconv.Atoi(id)
	s.NoError(err)

	// build Principal headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "POSITION_TYPE", "ADDRESS"}

	// build first record
	values := []string{"Zurn Concierge Nursing, Inc.", id, "", "TTeri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// add principal
	resp, err := schClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	s.NoError(err)

	// validate principal
	bp := resp.GetPrincipal()
	s.Equal(bp.Id, id)
	s.Equal(int(bp.EntityId), num)

	// get principal
	gResp, err := schClient.GetEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_PRINCIPAL,
		Id:   bp.Id,
	})
	s.NoError(err)
	s.Equal(gResp.GetPrincipal().Id, bp.Id)

	// delete principal
	dResp, err := schClient.DeleteEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_PRINCIPAL,
		Id:   bp.Id,
	})
	s.NoError(err)
	s.Equal(dResp.Ok, true)
}

func (s *BusinessActivitiesTestSuite) Test_GetCSVHeadersActivity_MissingReqParams() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})

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

	// test for missing request params
	s.testForMissingRequestParams(env, bizwkfl.GetCSVHeadersActivity)
}

func (s *BusinessActivitiesTestSuite) Test_GetCSVHeadersActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})

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

	// test for csv file read headers
	s.testGetCSVHeadersActivity(env)
}

func (s *BusinessActivitiesTestSuite) Test_GetCSVOffsetsActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: CSVOffsetsActivityAlias})

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

	// test for csv file offsets
	s.testGetCSVOffsetsActivity(env)
}

func (s *BusinessActivitiesTestSuite) Test_ReadCSVActivity() {
	// get test logger
	l := s.getTestLogger()

	// set environment logger
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity, ReadCSVActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: CSVOffsetsActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.ReadCSVActivity, activity.RegisterOptions{Name: ReadCSVActivityAlias})

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

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.SetTestTimeout(comwkfl.ONE_DAY)

	// test for csv file read
	s.testCSVRead(env)
}

func (s *BusinessActivitiesTestSuite) testForMissingRequestParams(env *testsuite.TestActivityEnvironment, activityFn bizwkfl.CSVActivityFn) {
	reqstr := "missing-request-params-test@gmail.com"
	reqInfo := &models.CSVInfo{
		FileName:    "",
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*temporal.ActivityError)
	s.Equal(true, ok)
	cause := cusErr.Unwrap()
	appErr, ok := cause.(*temporal.ApplicationError)
	s.Equal(true, ok)
	s.Equal(comwkfl.ERR_MISSING_FILE_NAME, appErr.Type())

	filePath := fmt.Sprintf("%s/%s", DATA_PATH, MISSING_FILE_NAME)
	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: "",
		Type:        models.AGENT,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*temporal.ActivityError)
	s.Equal(true, ok)
	cause = cusErr.Unwrap()
	appErr, ok = cause.(*temporal.ApplicationError)
	s.Equal(true, ok)
	s.Equal(comwkfl.ERR_MISSING_REQSTR, appErr.Type())

	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*temporal.ActivityError)
	s.Equal(true, ok)
	cause = cusErr.Unwrap()
	appErr, ok = cause.(*temporal.ApplicationError)
	s.Equal(true, ok)
	s.Equal(comwkfl.ERR_MISSING_FILE, appErr.Type())
}

func (s *BusinessActivitiesTestSuite) testGetCSVHeadersActivity(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	reqstr := "get-csv-headers-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}
	l.Debug("csv file read headers request", slog.Any("req", req))
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", slog.Any("result", result))
}

func (s *BusinessActivitiesTestSuite) testGetCSVOffsetsActivity(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	reqstr := "get-csv-offsets-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(8, len(result.Headers.Headers))
	s.Equal("ENTITY_NAME", result.Headers.Headers[0])
	s.Equal("AGENT_TYPE", result.Headers.Headers[7])
	l.Info("csv file read headers response", slog.Any("error", err), slog.Any("response", result))

	resp, err = env.ExecuteActivity(bizwkfl.GetCSVOffsetsActivity, &result)
	s.NoError(err)

	var offResult models.CSVInfo
	err = resp.Get(&offResult)
	s.NoError(err)
	s.Equal(result.Headers.Offset, offResult.OffSets[0])
	l.Info("csv file offsets response", slog.Any("error", err), slog.Any("response", offResult))
}

func (s *BusinessActivitiesTestSuite) testCSVRead(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	isAgent := false
	reqstr := "csv-file-read-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	if isAgent {
		s.Equal(8, len(result.Headers.Headers))
		s.Equal("ENTITY_NAME", result.Headers.Headers[0])
		s.Equal("AGENT_TYPE", result.Headers.Headers[7])
	}

	resp, err = env.ExecuteActivity(bizwkfl.GetCSVOffsetsActivity, &result)
	s.NoError(err)

	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(result.Headers.Offset, result.OffSets[0])

	resp, err = env.ExecuteActivity(bizwkfl.ReadCSVActivity, &result)
	s.NoError(err)

	err = resp.Get(&result)
	s.NoError(err)
	// s.Equal(25, result.Count)
	l.Info("csv read csv response", slog.Any("result", result))
}

func (s *BusinessActivitiesTestSuite) getTestLogger() *slog.Logger {
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
