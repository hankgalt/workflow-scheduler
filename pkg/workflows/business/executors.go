package business

import (
	"time"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

func ExecuteAddAgentActivity(ctx workflow.Context, fields map[string]string) (string, error) {
	// setup activity options
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 5,
		MaximumAttempts:    3,
		NonRetriableErrorReasons: []string{
			ERR_ADDING_AGENT,
			common.ERR_MISSING_SCHEDULER_CLIENT,
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
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 5,
		MaximumAttempts:    3,
		NonRetriableErrorReasons: []string{
			ERR_ADDING_PRINCIPAL,
			common.ERR_MISSING_SCHEDULER_CLIENT,
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
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 5,
		MaximumAttempts:    3,
		NonRetriableErrorReasons: []string{
			ERR_ADDING_FILING,
			common.ERR_MISSING_SCHEDULER_CLIENT,
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
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 5,
		MaximumAttempts:    10,
		NonRetriableErrorReasons: []string{
			common.ERR_WRONG_HOST,
			common.ERR_MISSING_FILE_NAME,
			common.ERR_MISSING_REQSTR,
			common.ERR_MISSING_FILE,
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
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: common.FIVE_MINS,
		MaximumAttempts:    10,
		NonRetriableErrorReasons: []string{
			common.ERR_WRONG_HOST,
			common.ERR_MISSING_FILE_NAME,
			common.ERR_MISSING_REQSTR,
			common.ERR_MISSING_FILE,
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
	ao := common.DefaultActivityOptions()
	ao.StartToCloseTimeout = common.ONE_DAY
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: common.FIVE_MINS,
		MaximumAttempts:    10,
		NonRetriableErrorReasons: []string{
			common.ERR_WRONG_HOST,
			common.ERR_MISSING_FILE_NAME,
			common.ERR_MISSING_REQSTR,
			ERR_MISSING_OFFSETS,
			common.ERR_MISSING_FILE,
			ERR_MISSING_START_OFFSET,
			ERR_BUILDING_OFFSETS,
			common.ERR_MISSING_SCHEDULER_CLIENT,
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
