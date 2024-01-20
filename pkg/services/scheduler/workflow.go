package scheduler

import (
	"context"
	"fmt"

	"go.uber.org/cadence/client"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

func (ss *schedulerService) ProcessFileSignalWorkflow(ctx context.Context, params *models.FileSignalParams) (*models.WorkflowRun, error) {
	if params.FilePath == "" || params.RequestedBy == "" || params.Type == "" {
		ss.Error(ERR_MISSING_REQUIRED, zap.Any("params", params))
		return nil, ErrMissingRequired
	}

	req := &models.CSVInfo{
		FileName:    params.FilePath,
		RequestedBy: params.RequestedBy,
		Type:        params.Type,
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                              fmt.Sprintf("file-%s", params.FilePath),
		TaskList:                        bizwkfl.ApplicationName,
		ExecutionStartToCloseTimeout:    common.ONE_DAY,
		DecisionTaskStartToCloseTimeout: common.FIVE_MINS, // set to max, as there are decision tasks that'll take as long as max
		WorkflowIDReusePolicy:           1,
	}
	we, err := ss.cadence.StartWorkflow(workflowOptions, bizwkfl.ProcessFileSignalWorkflow, req)
	if err != nil {
		return nil, err
	}

	return &models.WorkflowRun{
		RunId:      we.RunID,
		WorkflowId: we.ID,
	}, nil
}

func (ss *schedulerService) QueryWorkflowState(ctx context.Context, params *models.WorkflowQueryParams) (interface{}, error) {
	if params.WorkflowId == "" || params.RunId == "" {
		ss.Error(ERR_MISSING_REQUIRED, zap.Any("params", params))
		return nil, ErrMissingRequired
	}

	if state, err := ss.cadence.QueryWorkflow(ctx, params.WorkflowId, params.RunId, "state"); err != nil {
		return nil, err
	} else {
		return state, nil
	}
}
