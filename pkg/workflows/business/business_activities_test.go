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
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const (
	ActivityAlias           string = "some-random-activity-alias"
	CSVHeadersActivityAlias string = "get-csv-headers-activity-alias"
	CSVOffsetsActivityAlias string = "get-csv-offsets-activity-alias"
	ReadCSVActivityAlias    string = "read-csv-activity-alias"

	DownloadFileActivityAlias string = "download-file-activity-alias"
	UploadFileActivityAlias   string = "upload-file-activity-alias"
	DeleteFileActivityAlias   string = "delete-file-activity-alias"
)

const DATA_PATH string = "scheduler"
const MISSING_FILE_NAME string = "geo.csv"
const LIVE_FILE_NAME string = "Filings-sm.csv"

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

func (s *BusinessWorkflowTestSuite) Test_GetCSVHeadersActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})

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

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: CSVOffsetsActivityAlias})

	// test for csv file offsets
	s.testGetCSVOffsetsActivity(env)
}

func (s *BusinessWorkflowTestSuite) Test_ReadCSVActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetCSVOffsetsActivity, ReadCSVActivity
	env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: CSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: CSVOffsetsActivityAlias})
	env.RegisterActivityWithOptions(bizwkfl.ReadCSVActivity, activity.RegisterOptions{Name: ReadCSVActivityAlias})

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(common.ERR_SCH_CLIENT_INIT, zap.Error(err))
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

	isAgent := false
	reqstr := "get-csv-headers-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}
	l.Debug("csv file read headers request", zap.Any("req", req))
	resp, err := env.ExecuteActivity(bizwkfl.GetCSVHeadersActivity, req)
	s.NoError(err)

	var result models.CSVInfo
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", zap.Any("result", result))
	if isAgent {
		s.Equal(8, len(result.Headers.Headers))
		s.Equal("ENTITY_NAME", result.Headers.Headers[0])
		s.Equal("AGENT_TYPE", result.Headers.Headers[7])
	}
	l.Info("csv file read headers response", zap.Any("response", result))
}

func (s *BusinessWorkflowTestSuite) testGetCSVOffsetsActivity(env *testsuite.TestActivityEnvironment) {
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

	isAgent := false
	reqstr := "csv-file-read-test@gmail.com"
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
	l.Info("csv read csv response", zap.Error(err), zap.Any("result", result))
}

func (s *BusinessWorkflowTestSuite) testForMissingRequestParams(env *testsuite.TestActivityEnvironment, activityFn bizwkfl.CSVActivityFn) {
	reqstr := "missing-request-params-test@gmail.com"
	reqInfo := &models.CSVInfo{
		FileName:    "",
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), common.ERR_MISSING_FILE_NAME)

	filePath := fmt.Sprintf("%s/%s", DATA_PATH, MISSING_FILE_NAME)
	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: "",
		Type:        models.AGENT,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), common.ERR_MISSING_REQSTR)

	reqInfo = &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), common.ERR_MISSING_FILE)
}
