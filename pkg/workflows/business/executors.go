package business

import (
	"time"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func ExecuteAddAgentActivity(ctx workflow.Context, fields map[string]string) (string, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
		NonRetryableErrorTypes: []string{
			ERR_ADDING_AGENT,
			comwkfl.ERR_MISSING_SCHEDULER_CLIENT,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var agentId string
	err := workflow.ExecuteActivity(ctx, AddAgentActivity, fields).Get(ctx, &agentId)
	if err != nil {
		return "", err
	}
	return agentId, nil
}

func ExecuteAddPrincipalActivity(ctx workflow.Context, fields map[string]string) (string, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
		NonRetryableErrorTypes: []string{
			ERR_ADDING_PRINCIPAL,
			comwkfl.ERR_MISSING_SCHEDULER_CLIENT,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var principalId string
	err := workflow.ExecuteActivity(ctx, AddPrincipalActivity, fields).Get(ctx, &principalId)
	if err != nil {
		return "", err
	}
	return principalId, nil
}

func ExecuteAddFilingActivity(ctx workflow.Context, fields map[string]string) (string, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
		NonRetryableErrorTypes: []string{
			ERR_ADDING_FILING,
			comwkfl.ERR_MISSING_SCHEDULER_CLIENT,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var filingId string
	err := workflow.ExecuteActivity(ctx, AddFilingActivity, fields).Get(ctx, &filingId)
	if err != nil {
		return "", err
	}
	return filingId, nil
}

func ExecuteGetCSVHeadersActivity(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			comwkfl.ERR_WRONG_HOST,
			comwkfl.ERR_MISSING_FILE_NAME,
			comwkfl.ERR_MISSING_REQSTR,
			comwkfl.ERR_MISSING_FILE,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp models.CSVInfo
	err := workflow.ExecuteActivity(ctx, GetCSVHeadersActivity, req).Get(ctx, &resp)
	if err != nil {
		return req, err
	}
	return &resp, nil
}

func ExecuteGetCSVOffsetsActivity(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			comwkfl.ERR_WRONG_HOST,
			comwkfl.ERR_MISSING_FILE_NAME,
			comwkfl.ERR_MISSING_REQSTR,
			comwkfl.ERR_MISSING_FILE,
			ERR_MISSING_START_OFFSET,
			ERR_BUILDING_OFFSETS,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp models.CSVInfo
	err := workflow.ExecuteActivity(ctx, GetCSVOffsetsActivity, req).Get(ctx, &resp)
	if err != nil {
		return req, err
	}
	return &resp, nil
}

func ExecuteReadCSVActivity(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	// setup activity options
	ao := comwkfl.DefaultActivityOptions()
	ao.StartToCloseTimeout = comwkfl.ONE_DAY
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			comwkfl.ERR_WRONG_HOST,
			comwkfl.ERR_MISSING_FILE_NAME,
			comwkfl.ERR_MISSING_REQSTR,
			ERR_MISSING_OFFSETS,
			comwkfl.ERR_MISSING_FILE,
			ERR_MISSING_START_OFFSET,
			ERR_BUILDING_OFFSETS,
			comwkfl.ERR_MISSING_SCHEDULER_CLIENT,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp models.CSVInfo
	err := workflow.ExecuteActivity(ctx, ReadCSVActivity, req).Get(ctx, &resp)
	if err != nil {
		return req, err
	}
	return &resp, nil
}
