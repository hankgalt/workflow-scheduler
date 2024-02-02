package file

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common"
)

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

func defaultFileActivityOptions() workflow.ActivityOptions {
	ao := comwkfl.DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			comwkfl.ERR_WRONG_HOST,
			comwkfl.ERR_MISSING_SCHEDULER_CLIENT,
			ERR_MISSING_CLOUD_CLIENT,
			ERR_MISSING_CLOUD_BUCKET,
			comwkfl.ERR_MISSING_FILE_NAME,
			comwkfl.ERR_MISSING_REQSTR,
			ERR_FILE_DOWNLOAD,
		},
	}
	return ao
}
