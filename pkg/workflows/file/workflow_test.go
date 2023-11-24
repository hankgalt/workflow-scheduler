package file_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
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

	s.env.RegisterWorkflow(file.CreateFileWorkflow)
	s.env.RegisterWorkflow(file.UploadFileWorkflow)
	s.env.RegisterWorkflow(file.DownloadFileWorkflow)
	s.env.RegisterWorkflow(file.CreateUploadFileWorkflow)
	s.env.RegisterWorkflow(file.CreateUploadDownloadFileWorkflow)
	s.env.RegisterWorkflow(file.DryRunWorkflow)
	s.env.RegisterWorkflow(file.ReadCSVWorkflow)

	s.env.RegisterActivityWithOptions(common.CreateRunActivity, activity.RegisterOptions{
		Name: common.CreateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(file.CreateFileActivity, activity.RegisterOptions{
		Name: file.CreateFileActivityName,
	})
	s.env.RegisterActivityWithOptions(common.UpdateRunActivity, activity.RegisterOptions{
		Name: common.UpdateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(file.UploadFileActivity, activity.RegisterOptions{
		Name: file.UploadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(file.DownloadFileActivity, activity.RegisterOptions{
		Name: file.DownloadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(file.DryRunActivity, activity.RegisterOptions{
		Name: file.DryRunActivityName,
	})
	s.env.RegisterActivityWithOptions(file.ReadCSVActivity, activity.RegisterOptions{
		Name: file.ReadCSVActivityName,
	})

	logger := logger.NewTestAppZapLogger(TEST_DIR)
	cloudCfg, err := cloud.NewCloudConfig("", TEST_DIR)
	if err != nil {
		logger.Error("error initializing cloud client config", zap.Error(err))
		panic(err)
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, logger)
	if err != nil {
		logger.Error("error initializing cloud client", zap.Error(err))
		panic(err)
	}
	s.cloudClient = cloudClient

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		logger.Error("environment missing cloud bucket name")
		panic(err)
	}

	bizClient, err := scheduler.NewClient(logger, scheduler.NewDefaultClientOption())
	if err != nil {
		logger.Error("error initializing business grpc client", zap.Error(err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, bizClient)
	ctx = context.WithValue(ctx, cloud.CloudClientContextKey, cloudClient)
	ctx = context.WithValue(ctx, cloud.CloudBucketContextKey, bucket)
	ctx = context.WithValue(ctx, file.DataPathContextKey, TEST_DIR)
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

func (s *FileWorkflowTestSuite) Test_ReadCSVWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "Agents-sm.csv")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}
	expectedCall := []string{
		file.ReadCSVActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// read csv file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.ReadCSVWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_CreateFileWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "test.json")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}
	expectedCall := []string{
		common.CreateRunActivityName,
		file.CreateFileActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[1]:
			// create file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[2]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.CreateFileWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_CreateUploadFileWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "data.json")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}
	expectedCall := []string{
		common.CreateRunActivityName,
		file.CreateFileActivityName,
		common.UpdateRunActivityName,
		file.UploadFileActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[1]:
			// create file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[2]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[3]:
			// upload file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[4]:
			// updte run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.CreateUploadFileWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_CreateUploadDownloadFileWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "test.json")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}
	expectedCall := []string{
		common.CreateRunActivityName,
		file.CreateFileActivityName,
		common.UpdateRunActivityName,
		file.UploadFileActivityName,
		common.UpdateRunActivityName,
		file.DownloadFileActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[1]:
			// create file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[2]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[3]:
			// upload file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[4]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[5]:
			// download file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[6]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.CreateUploadDownloadFileWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_DryRunWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "test.json")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}
	expectedCall := []string{
		common.CreateRunActivityName,
		file.CreateFileActivityName,
		common.UpdateRunActivityName,
		file.UploadFileActivityName,
		common.UpdateRunActivityName,
		file.DownloadFileActivityName,
		common.UpdateRunActivityName,
		file.DryRunActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[1]:
			// create file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[2]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[3]:
			// upload file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[4]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[5]:
			// download file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[6]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[7]:
			// dry run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[8]:
			// update run
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.DryRunWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_DownloadFileWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "starbucks.json")
	expectedCall := []string{
		file.DownloadFileActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			var input string
			s.NoError(args.Get(&input))
			s.Equal(filePath, input)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.DownloadFileWorkflow, filePath)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}

func (s *FileWorkflowTestSuite) Test_UploadFileWorkflow() {
	filePath := fmt.Sprintf("%s/%s", "data", "test1.json")
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "vtalwar.w@gmail.com",
	}

	expectedCall := []string{
		file.CreateFileActivityName,
		file.UploadFileActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// create file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		case expectedCall[1]:
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("unexpected activity call")
		}
	})
	s.env.ExecuteWorkflow(file.UploadFileWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}
