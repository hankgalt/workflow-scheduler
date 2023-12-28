package file_test

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/encoded"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

func (s *FileWorkflowTestSuite) Test_DeleteFileWorkflow() {
	l := logger.NewTestAppZapLogger(TEST_DIR)

	filePath := fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME)
	reqInfo := &models.RequestInfo{
		FileName:    filePath,
		RequestedBy: "delete-file-workflow-test@gmail.com",
	}

	expectedCall := []string{
		fiwkfl.DeleteFileActivityName,
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
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(reqInfo.FileName, input.FileName)
		default:
			panic("Test_DeleteFileWorkflow unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_DeleteFileWorkflow panicked", zap.Any("error", err), zap.String("wkfl", fiwkfl.DeleteFileWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_DeleteFileWorkflow error", zap.Any("error", err))
		} else {
			var result models.RequestInfo
			s.env.GetWorkflowResult(&result)
			l.Info("Test_DeleteFileWorkflow result", zap.Any("file", result.FileName))
		}

	}()

	s.env.ExecuteWorkflow(fiwkfl.DeleteFileWorkflow, reqInfo)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(expectedCall, activityCalled)
}
