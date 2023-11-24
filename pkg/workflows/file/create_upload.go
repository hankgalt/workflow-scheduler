package file

import (
	"time"

	"github.com/comfforts/errors"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

// ApplicationName is the task list for this workflow
const CreateUploadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.CreateUploadFileWorkflow"

// CreateFileWorkflow workflow decider
func CreateUploadFileWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
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
	logger.Info("CreateUploadFileWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	var resp *models.RequestInfo
	for i := 1; i < 3; i++ {
		resp, err = createUploadFile(ctx, req)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("CreateUploadFileWorkflow failed.", zap.String("Error", err.Error()))
		return nil, err
	}

	logger.Info("CreateUploadFileWorkflow completed.")
	return resp, nil
}

func createUploadFile(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("createUploadFile failed - error getting session context", zap.String("Error", err.Error()))
		return nil, errors.WrapError(err, "createUploadFile failed - error getting session context")
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
		logger.Error("createUploadFile failed - error creating file", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadFile - workflow file created", zap.String("filepath", fileInfo.FileName))

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
		logger.Error("createUploadFile failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadFile - run status updated", zap.String("status", upRunInfo.Status))

	var respInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, UploadFileActivity, upRunInfo).Get(sessionCtx, &respInfo)
	if err != nil {
		logger.Error("createUploadFile failed - error uploading file", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadFile - workflow file uploaded", zap.String("filepath", respInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    respInfo.FileName,
		Org:         respInfo.Org,
		RequestedBy: respInfo.RequestedBy,
		HostID:      respInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "UPLOADED",
	}
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upRunInfo)
	if err != nil {
		logger.Error("createUploadFile failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadFile - run status updated", zap.String("status", upRunInfo.Status))

	return upRunInfo, nil
}
