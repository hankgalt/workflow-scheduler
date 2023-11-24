package common

import (
	"context"

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
)

const (
	CREATE_RUN_ACT_STARTED = "create run activity started."
	ERR_CREATING_RUN       = "error creating workflow run"
	CREATE_RUN_ACT_COMPL   = "create run activity completed."
	UPDATE_RUN_ACT_STARTED = "update run activity started."
	ERR_UPDATING_RUN       = "error updating workflow run"
	UPDATE_RUN_ACT_COMPL   = "update run activity completed."
)

var (
	ErrCreatingRun = errors.NewAppError(ERR_CREATING_RUN)
	ErrUpdatingRun = errors.NewAppError(ERR_UPDATING_RUN)
)

func CreateRunActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx)
	logger.Info(CREATE_RUN_ACT_STARTED, zap.Any("req", req))

	bizClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if bizClient == nil {
		logger.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return req, ErrMissingSchClient
	}

	resp, err := bizClient.CreateRun(ctx, &api.RunRequest{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		RequestedBy: req.RequestedBy,
	})
	if err != nil {
		logger.Error(ERR_CREATING_RUN, zap.Error(err))
		return nil, ErrCreatingRun
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
		HostID:      req.HostID,
		RunId:       resp.Run.RunId,
		WorkflowId:  resp.Run.WorkflowId,
		Status:      resp.Run.Status,
	}

	logger.Info(CREATE_RUN_ACT_COMPL, zap.Any("req", req))
	return acResp, nil
}

func UpdateRunActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx)
	logger.Info(UPDATE_RUN_ACT_STARTED, zap.Any("req", req))

	bizClient := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if bizClient == nil {
		logger.Error(ERR_MISSING_SCHEDULER_CLIENT)
		return req, ErrMissingSchClient
	}

	resp, err := bizClient.UpdateRun(ctx, &api.UpdateRunRequest{
		RunId:      req.RunId,
		WorkflowId: req.WorkflowId,
		Status:     req.Status,
	})
	if err != nil {
		logger.Error(ERR_UPDATING_RUN, zap.Error(err))
		return nil, ErrUpdatingRun
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
		HostID:      req.HostID,
		RunId:       resp.Run.RunId,
		WorkflowId:  resp.Run.WorkflowId,
		Status:      resp.Run.Status,
	}
	logger.Info(UPDATE_RUN_ACT_COMPL, zap.String("file-path", req.FileName), zap.String("status", resp.Run.Status))
	return acResp, nil
}
