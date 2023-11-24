package common

import (
	"time"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

type ActivityParams struct {
	RunId, WkflId, Reqstr string
}

type RunActivityParams struct {
	ActivityParams
	Status string
}

func DefaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
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
}

func NoRetryActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 2, // such a short timeout to make sample fail over very fast
	}
}

func GetRunWorkflowIds(ctx workflow.Context, runId, wkflId string) (string, string) {
	if runId == "" {
		runId = workflow.GetInfo(ctx).WorkflowExecution.RunID
	}

	if wkflId == "" {
		wkflId = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	return runId, wkflId
}

func ExecuteCreateRunActivity(ctx workflow.Context, params *RunActivityParams) (*models.RequestInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, CreateRunActivityName, &models.RequestInfo{
		RunId:       params.RunId,
		WorkflowId:  params.WkflId,
		RequestedBy: params.Reqstr,
	}).Get(ctx, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func ExecuteUpdateRunActivity(ctx workflow.Context, params *RunActivityParams) (*models.RequestInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, UpdateRunActivityName, &models.RequestInfo{
		RunId:       params.RunId,
		WorkflowId:  params.WkflId,
		Status:      params.Status,
		RequestedBy: params.Reqstr,
	}).Get(ctx, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func ExecuteUpdateFileRunActivity(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, UpdateRunActivityName, req).Get(ctx, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
