package file

import (
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence/workflow"
)

func ExecuteReadCSVActivity(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	// setup activity options
	ao := common.DefaultActivityOptions()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var fileInfo *models.RequestInfo
	err := workflow.ExecuteActivity(ctx, ReadCSVActivity, req).Get(ctx, &fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}
