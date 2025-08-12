package batch

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
)

func DefaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Minute * 10,
		HeartbeatTimeout:       time.Second * 2, // such a short timeout to make sample fail over very fast
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second * 5,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Minute,
			NonRetryableErrorTypes: []string{"bad-error"},
		},
	}
}

func DefaultRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    time.Second * 5,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute * 10,
		MaximumAttempts:    3,
		NonRetryableErrorTypes: []string{
			ERROR_INVALID_CONFIG_TYPE,
		},
	}
}

func ExecuteSetupLocalCSVBatchActivity(
	ctx workflow.Context,
	handlerCfg batch.LocalCSVBatchConfig,
	reqCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()
	// ao.RetryPolicy.NonRetryableErrorTypes = append(ao.RetryPolicy.NonRetryableErrorTypes, ERROR_INVALID_CONFIG_TYPE)

	ctx = workflow.WithActivityOptions(ctx, ao)

	l := workflow.GetLogger(ctx)
	l.Debug("ExecuteSetupLocalCSVBatchActivity - started", "handlerCfg", handlerCfg, "reqCfg", reqCfg)

	var resp batch.RequestConfig
	fut := workflow.ExecuteActivity(ctx, SetupLocalCSVBatch, handlerCfg, reqCfg)
	err := fut.Get(ctx, &resp)
	if err != nil {
		l.Error("failed to execute SetupLocalCSVBatch activity", "error", err)
		return reqCfg, err
	}
	return &resp, nil
}

func AsyncExecuteHandleLocalCSVBatchDataActivity(
	ctx workflow.Context,
	cfg batch.LocalCSVBatchConfig,
	reqCfg *batch.RequestConfig,
	bat *batch.Batch,
) workflow.Future {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()

	ctx = workflow.WithActivityOptions(ctx, ao)

	return workflow.ExecuteActivity(ctx, HandleLocalCSVBatchData, cfg, reqCfg, bat)
}

func ExecuteSetupCloudCSVBatchActivity(
	ctx workflow.Context,
	handlerCfg batch.CloudCSVBatchConfig,
	reqCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()

	ctx = workflow.WithActivityOptions(ctx, ao)

	l := workflow.GetLogger(ctx)
	l.Debug(
		"ExecuteSetupCloudCSVBatchActivity - started",
		"handlerCfg", handlerCfg,
		"reqCfg", reqCfg,
	)

	var resp batch.RequestConfig
	fut := workflow.ExecuteActivity(ctx, SetupCloudCSVBatch, handlerCfg, reqCfg)
	err := fut.Get(ctx, &resp)
	if err != nil {
		l.Error(
			"failed to execute SetupCloudCSVBatch activity",
			"error", err,
		)
		return reqCfg, err
	}
	return &resp, nil
}

func ExecuteSetupLocalCSVMongoBatchActivity(
	ctx workflow.Context,
	handlerCfg batch.LocalCSVMongoBatchConfig,
	reqCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()

	ctx = workflow.WithActivityOptions(ctx, ao)

	l := workflow.GetLogger(ctx)
	l.Debug(
		"ExecuteSetupLocalCSVMongoBatchActivity - started",
		"handlerCfg", handlerCfg,
		"reqCfg", reqCfg,
	)

	var resp batch.RequestConfig
	fut := workflow.ExecuteActivity(ctx, SetupLocalCSVMongoBatch, handlerCfg, reqCfg)
	err := fut.Get(ctx, &resp)
	if err != nil {
		l.Error(
			"ExecuteSetupLocalCSVMongoBatchActivity - failed to execute SetupLocalCSVMongoBatch activity",
			"error", err,
		)
		return reqCfg, err
	}
	return &resp, nil
}

func AsyncExecuteHandleLocalCSVMongoBatchDataActivity(
	ctx workflow.Context,
	cfg batch.LocalCSVMongoBatchConfig,
	reqCfg *batch.RequestConfig,
	bat *batch.Batch,
) workflow.Future {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()
	ctx = workflow.WithActivityOptions(ctx, ao)

	return workflow.ExecuteActivity(ctx, HandleLocalCSVMongoBatchData, cfg, reqCfg, bat)
}

func ExecuteSetupCloudCSVMongoBatchActivity(
	ctx workflow.Context,
	handlerCfg batch.CloudCSVMongoBatchConfig,
	reqCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()

	ctx = workflow.WithActivityOptions(ctx, ao)

	l := workflow.GetLogger(ctx)
	l.Debug(
		"ExecuteSetupCloudCSVMongoBatchActivity - started",
		"handlerCfg", handlerCfg,
		"reqCfg", reqCfg,
	)

	var resp batch.RequestConfig
	fut := workflow.ExecuteActivity(ctx, SetupCloudCSVMongoBatch, handlerCfg, reqCfg)
	err := fut.Get(ctx, &resp)
	if err != nil {
		l.Error(
			"ExecuteSetupCloudCSVMongoBatchActivity - failed to execute SetupCloudCSVMongoBatch activity",
			"error", err,
		)
		return reqCfg, err
	}
	return &resp, nil
}

func AsyncExecuteHandleCloudCSVMongoBatchDataActivity(
	ctx workflow.Context,
	cfg batch.CloudCSVMongoBatchConfig,
	reqCfg *batch.RequestConfig,
	bat *batch.Batch,
) workflow.Future {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = DefaultRetryPolicy()
	ctx = workflow.WithActivityOptions(ctx, ao)

	return workflow.ExecuteActivity(ctx, HandleCloudCSVMongoBatchData, cfg, reqCfg, bat)
}
