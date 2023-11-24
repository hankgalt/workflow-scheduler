package file

import (
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	api "github.com/hankgalt/workflow-scheduler/api/v1"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

// ApplicationName is the task list for this workflow
const ApplicationName = "FileProcessingGroup"

// ApplicationName is the task list for this workflow
const FileProcessingWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.FileProcessingWorkflow"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// FileProcessingWorkflow workflow decider
func FileProcessingWorkflow(ctx workflow.Context, req *api.FileRequest) (err error) {
	// setup activity options
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    10 * time.Minute,
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
	logger.Info("FileProcessingWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	reqInf := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
	}

	for i := 1; i < 5; i++ {
		err = processFile(ctx, reqInf)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("FileProcessingWorkflow failed.", zap.String("Error", err.Error()))
	} else {
		logger.Info("FileProcessingWorkflow completed.")
	}
	return err
}

func processFile(ctx workflow.Context, req *models.RequestInfo) (err error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		return err
	}
	defer workflow.CompleteSession(sessionCtx)

	var runInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, common.CreateRunActivityName, &models.RunUpdateParams{
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		RequestedBy: req.RequestedBy,
	}).Get(sessionCtx, runInfo)
	if err != nil {
		logger.Error("processFile failed - error creating workflow run", zap.Error(err))
		return err
	}
	logger.Info("processFile - workflow run created", zap.String("runId", runInfo.RunId))

	var cfInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, CreateFileActivityName, req.FileName).Get(sessionCtx, &cfInfo)
	if err != nil {
		logger.Error("processFile failed - error creating file", zap.Error(err), zap.String("Error", err.Error()))
		return err
	}
	logger.Info("processFile - file created", zap.String("filepath", cfInfo.FileName))

	var ufInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, UploadFileActivityName, cfInfo.FileName).Get(sessionCtx, &ufInfo)
	if err != nil {
		logger.Error("processFile failed - error uploading file", zap.Error(err), zap.String("Error", err.Error()))
		return err
	}
	logger.Info("processFile - file uploaded", zap.String("filepath", ufInfo.FileName))

	var dfInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DownloadFileActivityName, req.FileName).Get(sessionCtx, &dfInfo)
	if err != nil {
		logger.Error("processFile failed - error downloading file", zap.Error(err), zap.String("Error", err.Error()))
		return err
	}
	logger.Info("processFile - file downloaded", zap.String("filepath", dfInfo.FileName))

	var wkflRunUp *api.WorkflowRun
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &models.RunUpdateParams{
		RunId:      workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId: workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:     "DOWNLOADED",
	}).Get(sessionCtx, wkflRunUp)
	if err != nil {
		logger.Error("processFile failed - error updating run status", zap.Error(err))
		return err
	}
	logger.Info("processFile - run status updated", zap.String("status", wkflRunUp.Status))

	var fInfoDryRun *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DryRunActivityName, *dfInfo).Get(sessionCtx, &fInfoDryRun)
	if err != nil {
		logger.Error("processFile failed - dry run error", zap.Error(err), zap.String("Error", err.Error()))
		return err
	}
	logger.Info("processFile - dry run completed", zap.String("filepath", fInfoDryRun.FileName))

	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &models.RunUpdateParams{
		RunId:      workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId: workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:     "DRY_RUN_COMPLETED",
	}).Get(sessionCtx, wkflRunUp)
	if err != nil {
		logger.Error("processFile failed - error updating run status", zap.Error(err))
		return err
	}
	logger.Info("processFile - run status updated", zap.String("status", wkflRunUp.Status))
	return nil
}
