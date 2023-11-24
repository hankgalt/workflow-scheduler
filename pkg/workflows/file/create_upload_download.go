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
const CreateUploadDownloadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.CreateUploadDownloadFileWorkflow"

// CreateFileWorkflow workflow decider
func CreateUploadDownloadFileWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
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
	logger.Info("CreateUploadDownloadFileWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	var resp *models.RequestInfo
	for i := 1; i < 3; i++ {
		resp, err = createUploadDownloadFile(ctx, req)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("CreateUploadDownloadFileWorkflow failed.", zap.String("Error", err.Error()))
		return nil, err
	}

	logger.Info("CreateUploadDownloadFileWorkflow completed.")
	return resp, nil
}

func createUploadDownloadFile(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("createUploadDownloadFile failed - error getting session context", zap.String("Error", err.Error()))
		return nil, err
	}
	defer workflow.CompleteSession(sessionCtx)

	runId := workflow.GetInfo(ctx).WorkflowExecution.RunID
	wkflId := workflow.GetInfo(ctx).WorkflowExecution.ID

	runInfo, err := common.ExecuteCreateRunActivity(sessionCtx, &common.RunActivityParams{
		ActivityParams: common.ActivityParams{
			RunId:  runId,
			WkflId: wkflId,
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
		logger.Error("createUploadDownloadFile failed - error creating file", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadDownloadFile - workflow file created", zap.String("filepath", runInfo.FileName))

	upReq := models.RequestInfo{
		FileName:    fileInfo.FileName,
		Org:         fileInfo.Org,
		RequestedBy: fileInfo.RequestedBy,
		HostID:      fileInfo.HostID,
		RunId:       runId,
		WorkflowId:  wkflId,
		Status:      "CREATED",
	}
	upRunInfo, err := common.ExecuteUpdateFileRunActivity(sessionCtx, &upReq)
	if err != nil {
		logger.Error(common.ERR_UPDATING_WKFL_RUN, zap.Error(err))
		return req, err
	}
	logger.Info(common.WKFL_RUN_UPDATED, zap.String("status", upRunInfo.Status))

	var upInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, UploadFileActivity, upRunInfo).Get(sessionCtx, &upInfo)
	if err != nil {
		logger.Error("createUploadDownloadFile failed - error uploading file", zap.Error(err))
		return nil, err
	}
	logger.Info("createUploadDownloadFile - file uploaded", zap.String("filepath", upInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    upInfo.FileName,
		Org:         upInfo.Org,
		RequestedBy: upInfo.RequestedBy,
		HostID:      upInfo.HostID,
		RunId:       runId,
		WorkflowId:  wkflId,
		Status:      "UPLOADED",
	}
	upRunInfo, err = common.ExecuteUpdateFileRunActivity(sessionCtx, &upReq)
	if err != nil {
		logger.Error(common.ERR_UPDATING_WKFL_RUN, zap.Error(err))
		return req, err
	}
	logger.Info(common.WKFL_RUN_UPDATED, zap.String("status", upRunInfo.Status))

	var respInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, DownloadFileActivity, upInfo).Get(sessionCtx, &respInfo)
	if err != nil {
		logger.Error("createUploadDownloadFile failed - error downloding file", zap.String("Error", err.Error()))
		return nil, err
	}
	logger.Info("createUploadDownloadFile - file downloaded", zap.String("filepath", respInfo.FileName))

	upReq = models.RequestInfo{
		FileName:    respInfo.FileName,
		Org:         respInfo.Org,
		RequestedBy: respInfo.RequestedBy,
		HostID:      respInfo.HostID,
		RunId:       runId,
		WorkflowId:  wkflId,
		Status:      "DOWNLOADED",
	}
	upRunInfo, err = common.ExecuteUpdateFileRunActivity(sessionCtx, &upReq)
	if err != nil {
		logger.Error(common.ERR_UPDATING_WKFL_RUN, zap.Error(err))
		return req, err
	}
	logger.Info(common.WKFL_RUN_UPDATED, zap.String("status", upRunInfo.Status))

	return upRunInfo, nil
}
