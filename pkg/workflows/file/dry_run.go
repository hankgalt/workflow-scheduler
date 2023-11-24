package file

import (
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

// ApplicationName is the task list for this workflow
const DryRunWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DryRunWorkflow"

// CreateFileWorkflow workflow decider
func DryRunWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    5 * time.Minute,
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
	logger.Info("DryRunWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	var resp *models.RequestInfo
	for i := 1; i < 3; i++ {
		resp, err = dryRun(ctx, req)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("DryRunWorkflow failed.", zap.String("Error", err.Error()))
		return nil, err
	}

	logger.Info("DryRunWorkflow completed.")
	return resp, nil
}

func dryRun(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("dryRun failed - error getting session context", zap.String("Error", err.Error()))
		return nil, err
	}
	defer workflow.CompleteSession(sessionCtx)

	req.RunId = workflow.GetInfo(ctx).WorkflowExecution.RunID
	req.WorkflowId = workflow.GetInfo(ctx).WorkflowExecution.ID

	runInfo, err := common.ExecuteCreateRunActivity(sessionCtx, &common.RunActivityParams{
		ActivityParams: common.ActivityParams{
			RunId:  req.RunId,
			WkflId: req.WorkflowId,
			Reqstr: req.RequestedBy,
		},
	})
	if err != nil {
		logger.Error(common.ERR_CREATING_WKFL_RUN, zap.Error(err))
		return req, err
	}
	logger.Info(common.WKFL_RUN_CREATED, zap.String("run-id", runInfo.RunId))

	var fileInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, CreateFileActivity, runInfo).Get(sessionCtx, &fileInfo)
	if err != nil {
		logger.Error("dryRun failed - error creating file", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - workflow file created", zap.String("filepath", runInfo.FileName))

	upReq := models.RequestInfo{
		FileName:    fileInfo.FileName,
		Org:         fileInfo.Org,
		RequestedBy: fileInfo.RequestedBy,
		HostID:      fileInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "CREATED",
	}
	var upRunInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upRunInfo)
	if err != nil {
		logger.Error("dryRun failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - run status updated", zap.String("status", upRunInfo.Status))

	var upInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, UploadFileActivity, upRunInfo).Get(sessionCtx, &upInfo)
	if err != nil {
		logger.Error("dryRun failed - error uploading file", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - file uploaded", zap.String("filepath", upInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    upInfo.FileName,
		Org:         upInfo.Org,
		RequestedBy: upInfo.RequestedBy,
		HostID:      upInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "UPLOADED",
	}
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upInfo)
	if err != nil {
		logger.Error("dryRun failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - run status updated", zap.String("status", upInfo.Status))

	var respInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DownloadFileActivity, upInfo).Get(sessionCtx, &respInfo)
	if err != nil {
		logger.Error("dryRun failed - error downloding file", zap.String("Error", err.Error()))
		return nil, err
	}
	logger.Info("dryRun - file downloaded", zap.String("filepath", respInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    respInfo.FileName,
		Org:         respInfo.Org,
		RequestedBy: respInfo.RequestedBy,
		HostID:      respInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "DOWNLOADED",
	}
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upInfo)
	if err != nil {
		logger.Error("dryRun failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - run status updated", zap.String("status", upInfo.Status))

	var drInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DryRunActivityName, *upInfo).Get(sessionCtx, &drInfo)
	if err != nil {
		logger.Error("dryRun failed - dry run error", zap.Error(err), zap.String("Error", err.Error()))
		return nil, err
	}
	logger.Info("dryRun - dry run completed", zap.String("filepath", drInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    respInfo.FileName,
		Org:         respInfo.Org,
		RequestedBy: respInfo.RequestedBy,
		HostID:      respInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "DRY_RUN_COMPLETED",
	}
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upInfo)
	if err != nil {
		logger.Error("dryRun failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("dryRun - run status updated", zap.String("status", upInfo.Status))

	return upInfo, nil
}
