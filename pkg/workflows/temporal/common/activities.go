package common

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/comfforts/errors"

	api "github.com/hankgalt/workflow-scheduler/api/v1"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

/**
 * common activities used by all workflows.
 */
const (
	CreateRunActivityName = "CreateRunActivity"
	UpdateRunActivityName = "UpdateRunActivity"
	SearchRunActivityName = "SearchRunActivity"
)

func CreateRunActivity(ctx context.Context, req *models.RunParams) (*api.WorkflowRun, error) {
	l := activity.GetLogger(ctx)
	l.Info(CREATE_RUN_ACT_STARTED, zap.Any("req", req))

	schClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if schClient == nil {
		l.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return nil, ErrorMissingSchedulerClient
	}

	resp, err := schClient.CreateRun(ctx, &api.RunRequest{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		RequestedBy: req.RequestedBy,
		ExternalRef: req.ExternalRef,
		Type:        req.Type,
	})
	if err != nil {
		l.Error(ERR_CREATING_RUN, zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(ERR_CREATING_RUN, ERR_CREATING_RUN, errors.WrapError(err, ERR_CREATING_RUN))
	}

	l.Info(CREATE_RUN_ACT_COMPL, zap.Any("run", resp.Run))
	return resp.Run, nil
}

func UpdateRunActivity(ctx context.Context, req *models.RunParams) (*api.WorkflowRun, error) {
	l := activity.GetLogger(ctx)
	l.Info(UPDATE_RUN_ACT_STARTED, zap.Any("req", req))

	schClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if schClient == nil {
		l.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return nil, ErrorMissingSchedulerClient
	}

	resp, err := schClient.UpdateRun(ctx, &api.UpdateRunRequest{
		RunId:      req.RunId,
		WorkflowId: req.WorkflowId,
		Status:     req.Status,
	})
	if err != nil {
		l.Error(ERR_UPDATING_RUN, zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(ERR_UPDATING_RUN, ERR_UPDATING_RUN, errors.WrapError(err, ERR_UPDATING_RUN))
	}

	l.Info(UPDATE_RUN_ACT_COMPL, zap.String("status", resp.Run.Status))
	return resp.Run, nil
}

func SearchRunActivity(ctx context.Context, req *models.RunParams) ([]*api.WorkflowRun, error) {
	l := activity.GetLogger(ctx)
	l.Info("SearchRunActivity started", zap.Any("req", req))

	schClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if schClient == nil {
		l.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return nil, ErrorMissingSchedulerClient
	}

	resp, err := schClient.SearchRuns(ctx, &api.SearchRunRequest{
		Type: req.Type,
	})
	if err != nil {
		l.Error(ERR_SEARCH_RUN, zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(ERR_SEARCH_RUN, ERR_SEARCH_RUN, errors.WrapError(err, ERR_SEARCH_RUN))
	}

	l.Info("SearchRunActivity completed", zap.Any("resp", resp))
	return resp.Runs, nil
}
