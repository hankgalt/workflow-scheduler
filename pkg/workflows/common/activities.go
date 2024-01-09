package common

import (
	"context"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
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

const (
	CREATE_RUN_ACT_STARTED = "create run activity started."
	ERR_CREATING_RUN       = "error creating workflow run"
	ERR_SEARCH_RUN         = "error searching workflow run"
	CREATE_RUN_ACT_COMPL   = "create run activity completed."
	UPDATE_RUN_ACT_STARTED = "update run activity started."
	ERR_UPDATING_RUN       = "error updating workflow run"
	UPDATE_RUN_ACT_COMPL   = "update run activity completed."
)

var (
	ErrCreatingRun = errors.NewAppError(ERR_CREATING_RUN)
	ErrUpdatingRun = errors.NewAppError(ERR_UPDATING_RUN)
	ErrSearchRun   = errors.NewAppError(ERR_SEARCH_RUN)
)

func CreateRunActivity(ctx context.Context, req *models.RunParams) (*api.WorkflowRun, error) {
	logger := activity.GetLogger(ctx)
	logger.Info(CREATE_RUN_ACT_STARTED, zap.Any("req", req))

	schClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if schClient == nil {
		logger.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return nil, cadence.NewCustomError(ERR_MISSING_SCHEDULER_CLIENT, ErrMissingSchClient)
	}

	resp, err := schClient.CreateRun(ctx, &api.RunRequest{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		RequestedBy: req.RequestedBy,
		ExternalRef: req.ExternalRef,
		Type:        req.Type,
	})
	if err != nil {
		logger.Error(ERR_CREATING_RUN, zap.Error(err))
		return nil, cadence.NewCustomError(ERR_CREATING_RUN, err)
	}

	logger.Info(CREATE_RUN_ACT_COMPL, zap.Any("run", resp.Run))
	return resp.Run, nil
}

func UpdateRunActivity(ctx context.Context, req *models.RunParams) (*api.WorkflowRun, error) {
	l := activity.GetLogger(ctx)
	l.Info(UPDATE_RUN_ACT_STARTED, zap.Any("req", req))

	schClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if schClient == nil {
		l.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return nil, cadence.NewCustomError(ERR_MISSING_SCHEDULER_CLIENT, ErrMissingSchClient)
	}

	resp, err := schClient.UpdateRun(ctx, &api.UpdateRunRequest{
		RunId:      req.RunId,
		WorkflowId: req.WorkflowId,
		Status:     req.Status,
	})
	if err != nil {
		l.Error(ERR_UPDATING_RUN, zap.Error(err))
		return nil, cadence.NewCustomError(ERR_UPDATING_RUN, err)
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
		return nil, cadence.NewCustomError(ERR_MISSING_SCHEDULER_CLIENT, ErrMissingSchClient)
	}

	resp, err := schClient.SearchRuns(ctx, &api.SearchRunRequest{
		Type: req.Type,
	})
	if err != nil {
		l.Error(ERR_SEARCH_RUN, zap.Error(err))
		return nil, cadence.NewCustomError(ERR_SEARCH_RUN, err)
	}

	l.Info("SearchRunActivity completed", zap.Any("resp", resp))
	return resp.Runs, nil
}
