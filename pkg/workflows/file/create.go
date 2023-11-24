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
const CreateFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.CreateFileWorkflow"

// CreateFileWorkflow workflow decider
func CreateFileWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
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
	logger.Info("CreateFileWorkflow started.")

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities and the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	var err error
	var resp *models.RequestInfo
	for i := 1; i < 3; i++ {
		resp, err = createFile(ctx, req)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Error("CreateFileWorkflow failed.", zap.String("Error", err.Error()))
		return nil, err
	}

	logger.Info("CreateFileWorkflow completed.")
	return resp, nil
}

func createFile(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error("createFile failed - error getting session context", zap.String("Error", err.Error()))
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

	var respInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, CreateFileActivity, runInfo).Get(sessionCtx, &respInfo)
	if err != nil {
		logger.Error("createFile failed - error creating file", zap.String("Error", err.Error()))
		return nil, err
	}
	logger.Info("createFile - run status updated", zap.String("status", respInfo.Status))

	upReq := models.RequestInfo{
		FileName:    respInfo.FileName,
		Org:         respInfo.Org,
		RequestedBy: respInfo.RequestedBy,
		HostID:      respInfo.HostID,
		RunId:       workflow.GetInfo(ctx).WorkflowExecution.RunID,
		WorkflowId:  workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:      "CREATED",
	}
	var upRunInfo *models.RequestInfo
	err = workflow.ExecuteActivity(sessionCtx, common.UpdateRunActivityName, &upReq).Get(sessionCtx, &upRunInfo)
	if err != nil {
		logger.Error("createFile failed - error updating run status", zap.Error(err))
		return nil, err
	}
	logger.Info("createFile - run status updated", zap.String("status", upRunInfo.Status))

	return upRunInfo, nil
}
