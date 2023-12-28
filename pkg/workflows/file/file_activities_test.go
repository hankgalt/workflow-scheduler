package file_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/comfforts/logger"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

const (
	DownloadFileActivityAlias string = "download-file-activity-alias"
	UploadFileActivityAlias   string = "upload-file-activity-alias"
	DeleteFileActivityAlias   string = "delete-file-activity-alias"
)
const ERR_BUILD_REQ_CTX string = "error building file worker request context"
const DATA_PATH string = "scheduler"
const MISSING_FILE_NAME string = "geo.csv"
const LIVE_FILE_NAME string = "geo.json"

type Teardown func()

func TestFilePath(t *testing.T) {
	filePath := "/data/test.json"
	fmt.Println("filePath - ", filePath)

	dir := filepath.Dir(filePath)
	fmt.Println("dir - ", dir)

	base := filepath.Base(filePath)
	fmt.Println("base - ", base)

	ext := filepath.Ext(filePath)
	fmt.Println("ext - ", ext)

	fmt.Println("filePath - ", filePath)
}

func (s *FileWorkflowTestSuite) Test_LocalActivity() {
	localActivityFn := func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}

	env := s.NewTestActivityEnvironment()
	result, err := env.ExecuteLocalActivity(localActivityFn, "local_activity")
	s.NoError(err)
	var laResult string
	err = result.Get(&laResult)
	s.NoError(err)
	fmt.Println("LocalActivity Result: ", laResult)
	s.Equal("hello local_activity", laResult)
}

func (s *FileWorkflowTestSuite) Test_ActivityRegistration() {
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

func (s *FileWorkflowTestSuite) Test_DownloadFileActivity_MissingParams() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DownloadFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{Name: DownloadFileActivityAlias})

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for missing request params
	s.testForMissingRequestParams(env, fiwkfl.DownloadFileActivity)
}

func (s *FileWorkflowTestSuite) Test_DownloadFileActivity_MissingFile() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DownloadFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{Name: DownloadFileActivityAlias})

	// build test request context
	ctx, teardown, err := s.buildFileWorkerRequestContext()
	if err != nil {
		l.Error(ERR_BUILD_REQ_CTX, zap.Error(err))
		panic(err)
	}

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.testForMissingFile(env, fiwkfl.DownloadFileActivity)

	teardown()
}

func (s *FileWorkflowTestSuite) Test_DownloadFileActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DownloadFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{Name: DownloadFileActivityAlias})

	// build test request context
	ctx, teardown, err := s.buildFileWorkerRequestContext()
	if err != nil {
		l.Error(ERR_BUILD_REQ_CTX, zap.Error(err))
		panic(err)
	}

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.testDownloadFileActivity(env, fiwkfl.DownloadFileActivity)

	teardown()
}

func (s *FileWorkflowTestSuite) Test_UploadFileActivity_MissingParams() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register UploadFileActivity
	env.RegisterActivityWithOptions(fiwkfl.UploadFileActivity, activity.RegisterOptions{Name: UploadFileActivityAlias})

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for missing request params
	s.testForMissingRequestParams(env, fiwkfl.UploadFileActivity)
}

func (s *FileWorkflowTestSuite) Test_UploadFileActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register UploadFileActivity
	env.RegisterActivityWithOptions(fiwkfl.UploadFileActivity, activity.RegisterOptions{Name: UploadFileActivityAlias})

	// build test request context
	ctx, teardown, err := s.buildFileWorkerRequestContext()
	if err != nil {
		l.Error(ERR_BUILD_REQ_CTX, zap.Error(err))
		panic(err)
	}

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.testUploadFileActivity(env, fiwkfl.UploadFileActivity)

	teardown()
}

func (s *FileWorkflowTestSuite) Test_DeleteFileActivity_MissingParams() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DeleteFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DeleteFileActivity, activity.RegisterOptions{Name: DeleteFileActivityAlias})

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for missing request params
	s.testForMissingRequestParams(env, fiwkfl.DeleteFileActivity)
}

func (s *FileWorkflowTestSuite) Test_DeleteFileActivity_MissingFile() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DeleteFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DeleteFileActivity, activity.RegisterOptions{Name: DeleteFileActivityAlias})

	// build test request context
	ctx, teardown, err := s.buildFileWorkerRequestContext()
	if err != nil {
		l.Error(ERR_BUILD_REQ_CTX, zap.Error(err))
		panic(err)
	}

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.testForMissingCloudFile(env, fiwkfl.DeleteFileActivity)

	teardown()
}

func (s *FileWorkflowTestSuite) Test_DeleteFileActivity() {
	// get test logger
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register DeleteFileActivity
	env.RegisterActivityWithOptions(fiwkfl.DeleteFileActivity, activity.RegisterOptions{Name: DeleteFileActivityAlias})

	// build test request context
	ctx, teardown, err := s.buildFileWorkerRequestContext()
	if err != nil {
		l.Error(ERR_BUILD_REQ_CTX, zap.Error(err))
		panic(err)
	}

	// set worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.testDeleteFileActivity(env, fiwkfl.DeleteFileActivity)

	teardown()
}

func (s *FileWorkflowTestSuite) testForMissingRequestParams(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "missing-request-params-test@gmail.com"
	reqInfo := &models.RequestInfo{
		FileName:    "",
		RequestedBy: reqstr,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), common.ERR_MISSING_FILE_NAME)

	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	reqInfo = &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "",
	}
	_, err = env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok = err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), common.ERR_MISSING_REQSTR)
}

func (s *FileWorkflowTestSuite) testForMissingFile(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "missing-file-activity-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, MISSING_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), fiwkfl.ERR_FILE_DOWNLOAD)
}

func (s *FileWorkflowTestSuite) testDownloadFileActivity(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "download-file-activity-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}

	resp, err := env.ExecuteActivity(activityFn, reqInfo)
	s.NoError(err)

	var result models.RequestInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(reqInfo.FileName, result.FileName)
	s.Equal(reqInfo.RequestedBy, result.RequestedBy)
	s.Equal(result.HostID != "", true)
}

func (s *FileWorkflowTestSuite) testUploadFileActivity(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "upload-file-activity-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}

	resp, err := env.ExecuteActivity(activityFn, reqInfo)
	s.NoError(err)

	var result models.RequestInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(reqInfo.FileName, result.FileName)
	s.Equal(reqInfo.RequestedBy, result.RequestedBy)
	s.Equal(result.HostID != "", true)
}

func (s *FileWorkflowTestSuite) testForMissingCloudFile(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "missing-cloud-file-activity-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, MISSING_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}

	_, err := env.ExecuteActivity(activityFn, reqInfo)
	s.Error(err)
	cusErr, ok := err.(*cadence.CustomError)
	s.Equal(true, ok)
	s.Equal(cusErr.Reason(), fiwkfl.ERR_FILE_DELETE)
}

func (s *FileWorkflowTestSuite) testDeleteFileActivity(env *testsuite.TestActivityEnvironment, activityFn fiwkfl.FileActivityFn) {
	reqstr := "delete-file-activity-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
	}

	resp, err := env.ExecuteActivity(activityFn, reqInfo)
	s.NoError(err)

	var result models.RequestInfo
	err = resp.Get(&result)
	s.NoError(err)
	s.Equal(reqInfo.FileName, result.FileName)
	s.Equal(reqInfo.RequestedBy, result.RequestedBy)
	s.Equal(result.HostID != "", true)
}

func (s *FileWorkflowTestSuite) buildFileWorkerRequestContext() (context.Context, func(), error) {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	s.SetLogger(l)

	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = TEST_DIR
	}
	ctx := context.WithValue(context.Background(), fiwkfl.DataPathContextKey, dataPath)

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_BUCKET)
		return nil, nil, fiwkfl.ErrMissingCloudBucket
	}

	credsPath := os.Getenv("CREDS_PATH")
	if credsPath == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_CRED)
		return nil, nil, fiwkfl.ErrMissingCloudCred
	}

	ctx = context.WithValue(ctx, cloud.CloudBucketContextKey, bucket)

	cloudCfg, err := cloud.NewCloudConfig(credsPath, dataPath)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CFG_INIT, zap.Error(err))
		return nil, nil, err
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, l)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CLIENT_INIT, zap.Error(err))
		return nil, nil, err
	}
	ctx = context.WithValue(ctx, cloud.CloudClientContextKey, cloudClient)

	schClient, err := scheduler.NewClient(l, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error(common.ERR_SCH_CLIENT_INIT, zap.Error(err))
		if err := cloudClient.Close(); err != nil {
			l.Error(fiwkfl.ERR_CLOUD_CLIENT_CLOSE, zap.Error(err))
		}
		return nil, nil, err
	}
	ctx = context.WithValue(ctx, scheduler.SchedulerClientContextKey, schClient)

	teardown := func() {
		if err := cloudClient.Close(); err != nil {
			l.Error(fiwkfl.ERR_CLOUD_CLIENT_CLOSE, zap.Error(err))
		}
		if err := schClient.Close(); err != nil {
			l.Error(common.ERR_SCH_CLIENT_CLOSE, zap.Error(err))
		}
	}

	return ctx, teardown, nil
}
