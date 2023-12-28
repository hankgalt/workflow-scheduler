package file

import (
	"time"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

func ExecuteUploadFileActivity(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := defaultFileActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var fileInfo *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, UploadFileActivity, req).Get(ctx, &fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func ExecuteDownloadFileActivity(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := defaultFileActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var fileInfo *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, DownloadFileActivity, req).Get(ctx, &fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func ExecuteDeleteFileActivity(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := defaultFileActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var fileInfo *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, DeleteFileActivity, req).Get(ctx, &fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func defaultFileActivityOptions() workflow.ActivityOptions {
	ao := common.DefaultActivityOptions()
	ao.RetryPolicy = &cadence.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		ExpirationInterval: time.Minute * 2,
		MaximumAttempts:    10,
		NonRetriableErrorReasons: []string{
			common.ERR_WRONG_HOST,
			common.ERR_MISSING_SCHEDULER_CLIENT,
			ERR_MISSING_CLOUD_CLIENT,
			ERR_MISSING_CLOUD_BUCKET,
			common.ERR_MISSING_FILE_NAME,
			common.ERR_MISSING_REQSTR,
			ERR_FILE_DOWNLOAD,
		},
	}
	return ao
}
