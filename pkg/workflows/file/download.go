package file

import (
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

// ApplicationName is the task list for this workflow
const DownloadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DownloadFileWorkflow"

// DownloadFileWorkflow workflow decider
func DownloadFileWorkflow(ctx workflow.Context, filePath string) error {
	// setup activity options
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 2, // such a short timeout to make sample fail over very fast
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			MaximumInterval:          time.Minute,
			ExpirationInterval:       time.Minute * 10,
			NonRetriableErrorReasons: []string{"bad-error"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("DownloadFileWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	for i := 1; i < 3; i++ {
		err = downloadFile(ctx, filePath)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("DownloadFileWorkflow failed.", zap.String("Error", err.Error()))
		return err
	}

	logger.Info("DownloadFileWorkflow completed.")
	return nil
}

func downloadFile(ctx workflow.Context, filePath string) error {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("downloadFile failed - error getting session context", zap.String("Error", err.Error()))
		return errors.WrapError(err, "downloadFile failed - error getting session context")
	}
	defer workflow.CompleteSession(sessionCtx)

	var reqInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DownloadFileActivity, filePath).Get(sessionCtx, &reqInfo)
	if err != nil {
		logger.Error("downloadFile failed - error processing activity", zap.String("Error", err.Error()))
		return errors.WrapError(err, "downloadFile failed - error  processing activity")
	}
	return nil
}
