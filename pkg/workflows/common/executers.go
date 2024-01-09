package common

import (
	"fmt"
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

type ActivityParams struct {
	RunId, WkflId, Reqstr, Type, Status string
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

func ExecuteCreateRunActivity(ctx workflow.Context, params *models.RunParams) (*models.RequestInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, CreateRunActivityName, params).Get(ctx, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func ExecuteUpdateRunActivity(ctx workflow.Context, params *models.RunParams) (*models.RequestInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, UpdateRunActivityName, params).Get(ctx, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func ExecuteUpdateFileRunActivity(ctx workflow.Context, req *models.RunParams) (*models.RequestInfo, error) {
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

func ExecuteSearchRunActivity(ctx workflow.Context, params *models.RunParams) ([]*api.WorkflowRun, error) {
	fmt.Println("ExecuteSearchRunActivity started, params", params)
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 2,
		MaximumAttempts:    10,
		NonRetriableErrorReasons: []string{
			ERR_MISSING_SCHEDULER_CLIENT,
			ERR_SEARCH_RUN,
		},
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	var runs []*api.WorkflowRun
	err := workflow.ExecuteActivity(ctx, SearchRunActivityName, params).Get(ctx, &runs)
	if err != nil {
		return nil, err
	}

	return runs, nil
}
