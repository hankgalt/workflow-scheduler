package business_test

import (
	"context"
	"fmt"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"
	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

func (s *BusinessWorkflowTestSuite) Test_LocalActivity() {
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

func (s *BusinessWorkflowTestSuite) Test_ActivityRegistration() {
	activityFn := func(msg string) (string, error) {
		return msg, nil
	}
	activityAlias := "some-random-activity-alias"

	env := s.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: activityAlias})
	input := "some random input"

	encodedValue, err := env.ExecuteActivity(activityFn, input)
	s.NoError(err)
	output := ""
	encodedValue.Get(&output)
	s.Equal(input, output)

	encodedValue, err = env.ExecuteActivity(activityAlias, input)
	s.NoError(err)
	output = ""
	encodedValue.Get(&output)
	fmt.Println("output: ", output)
	s.Equal(input, output)
}

func (s *BusinessWorkflowTestSuite) Test_GetCSVHeadersActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// set GetCSVHeadersActivity alias
	csvHeadersActivityAlias := "get-csv-headers-activity-alias"

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: csvHeadersActivityAlias})

	// test for missing request params
	s.testForMissingRequestParams(env, bizwkfl.GetCSVHeadersActivity)

	// test for csv file read headers
	s.testGetCSVHeadersActivity(env)
}

func (s *BusinessWorkflowTestSuite) Test_GetCSVOffsetsActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// set GetCSVHeadersActivity, csvOffsetsActivityAlias aliases
	csvHeadersActivityAlias := "get-csv-headers-activity-alias"
	csvOffsetsActivityAlias := "get-csv-offsets-activity-alias"

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: csvHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: csvOffsetsActivityAlias})

	// test for csv file offsets
	s.testGetCSVOffsetsActivity(env)
}

func (s *BusinessWorkflowTestSuite) Test_ReadCSVActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// set GetCSVHeadersActivity, csvOffsetsActivityAlias, readCSVActivityAlias aliases
	csvHeadersActivityAlias := "get-csv-headers-activity-alias"
	csvOffsetsActivityAlias := "get-csv-offsets-activity-alias"
	readCSVActivityAlias := "read-csv-activity-alias"

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity, ReadCSVActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: csvHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: csvOffsetsActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.ReadCSVActivity, activity.RegisterOptions{Name: readCSVActivityAlias})

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("error initializing business grpc client", zap.Error(err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.SetTestTimeout(common.ONE_DAY)

	// test for csv file read
	s.testCSVRead(env)
}

func (s *BusinessWorkflowTestSuite) testGetCSVHeadersActivity(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	reqstr := "get-csv-headers-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", "data", "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        api.EntityType_AGENT,
	}
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(8, len(result.Headers.Headers))
	s.Equal("ENTITY_NAME", result.Headers.Headers[0])
	s.Equal("AGENT_TYPE", result.Headers.Headers[7])
	l.Info("csv file read headers response", zap.Error(err), zap.Any("response", result))
}

func (s *BusinessWorkflowTestSuite) testGetCSVOffsetsActivity(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	reqstr := "get-csv-offsets-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", "data", "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        api.EntityType_AGENT,
	}
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(8, len(result.Headers.Headers))
	s.Equal("ENTITY_NAME", result.Headers.Headers[0])
	s.Equal("AGENT_TYPE", result.Headers.Headers[7])
	l.Info("csv file read headers response", zap.Error(err), zap.Any("response", result))

	resp, err = env.ExecuteActivity(bizwkfl.GetCSVOffsetsActivity, &result)
	s.NoError(err)

	var offResult models.CSVInfo
	err = resp.Get(&offResult)
	s.NoError(err)
	s.Equal(result.Headers.Offset, offResult.OffSets[0])
	l.Info("csv file offsets response", zap.Error(err), zap.Any("response", offResult))
}

func (s *BusinessWorkflowTestSuite) testCSVRead(env *testsuite.TestActivityEnvironment) {
	l := s.GetLogger()

	reqstr := "csv-file-read-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", "data", "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        api.EntityType_AGENT,
	}
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(8, len(result.Headers.Headers))
	s.Equal("ENTITY_NAME", result.Headers.Headers[0])
	s.Equal("AGENT_TYPE", result.Headers.Headers[7])

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
	l.Info("csv read csv response", zap.Error(err), zap.Any("offsets", len(result.OffSets)))
}

func (s *BusinessWorkflowTestSuite) testForMissingRequestParams(env *testsuite.TestActivityEnvironment, activityFn bizwkfl.CSVActivityFn) {
	reqstr := "missing-request-params-test@gmail.com"
	reqInfo := &models.CSVInfo{
		FileName:    "",
		RequestedBy: reqstr,
		Type:        api.EntityType_AGENT,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), bizwkfl.ERR_MISSING_FILE_NAME)

	filePath := fmt.Sprintf("%s/%s", "data", "Agents-dummy.csv")
	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: "",
		Type:        api.EntityType_AGENT,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), bizwkfl.ERR_MISSING_REQSTR)

	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), bizwkfl.ERR_MISSING_FILE)
}
