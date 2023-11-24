package file

import (
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

// UploadFileWorkflowName is the task list for this workflow
const UploadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.UploadFileWorkflow"

// DownloadFileWorkflow workflow decider
func UploadFileWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second,
		StartToCloseTimeout:    15 * time.Minute,
		HeartbeatTimeout:       time.Second,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			MaximumInterval:          time.Minute,
			ExpirationInterval:       time.Minute * 2,
			NonRetriableErrorReasons: []string{"bad-error"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("UploadFileWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	var resp *models.RequestInfo
	for i := 1; i < 3; i++ {
		resp, err = uploadFile(ctx, req)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("UploadFileWorkflow failed.", zap.String("Error", err.Error()))
		return nil, err
	}

	logger.Info("UploadFileWorkflow completed.")
	return resp, nil
}

func uploadFile(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  1 * time.Minute,
		ExecutionTimeout: 20 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("uploadFile failed - error getting session context", zap.String("Error", err.Error()))
		return nil, err
	}
	defer workflow.CompleteSession(sessionCtx)

	var cfInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, CreateFileActivityName, req).Get(sessionCtx, &cfInfo)
	if err != nil {
		return nil, err
	}
	logger.Info("uploadFile - file created", zap.String("filepath", cfInfo.FileName))

	var ufInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, UploadFileActivityName, cfInfo).Get(sessionCtx, &ufInfo)
	if err != nil {
		logger.Error("uploadFile failed - error uploading file", zap.Error(err), zap.String("Error", err.Error()))
		return nil, err
	}
	logger.Info("uploadFile - file uploaded", zap.String("filepath", ufInfo.FileName))
	return ufInfo, nil
}
