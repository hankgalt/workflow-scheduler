package scheduler

import (
	"context"

	"github.com/comfforts/errors"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/zap"
)

const (
	ERR_RUN_CREATE    = "error: creating workflow run"
	ERR_RUN_FETCH     = "error: fetching workflow run details"
	ERR_RUN_UPDATE    = "error: updating workflow run"
	ERR_RUN_DELETE    = "error: deleting workflow run"
	ERR_DUPLICATE_RUN = "error: duplicate run"
)

var (
	ErrDuplicateRun = errors.NewAppError(ERR_DUPLICATE_RUN)
)

func (bs *schedulerService) CreateRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error) {
	wkflRun := models.WorkflowRun{
		WorkflowId:  params.WorkflowId,
		RunId:       params.RunId,
		Status:      string(models.STARTED),
		Type:        params.Type,
		ExternalRef: params.ExternalRef,
		CreatedBy:   params.RequestedBy,
	}

	res := bs.db.WithContext(ctx).Create(&wkflRun)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				return nil, ErrDuplicateRun
			}
		}
		return nil, errors.WrapError(res.Error, ERR_RUN_CREATE)
	}
	return &wkflRun, nil
}

func (bs *schedulerService) GetRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error) {
	var wkflRun models.WorkflowRun
	res := bs.db.WithContext(ctx).Where("run_id = ?", params.RunId).First(&wkflRun)
	if res.Error != nil {
		bs.Error(ERR_RUN_FETCH, zap.Error(res.Error), zap.String("run_id", params.RunId))
		return nil, errors.WrapError(res.Error, ERR_RUN_FETCH)
	}
	return &wkflRun, nil
}

func (bs *schedulerService) UpdateRun(ctx context.Context, params *models.RunParams) (*models.WorkflowRun, error) {
	wkflRun, err := bs.GetRun(ctx, params)
	if err != nil {
		return nil, err
	}

	res := bs.db.WithContext(ctx).Model(wkflRun).Updates(models.WorkflowRun{
		Status: params.Status,
	})
	if res.Error != nil {
		bs.Error(ERR_RUN_UPDATE, zap.Error(res.Error))
		return nil, err // retries?
	}
	return wkflRun, nil
}

func (bs *schedulerService) DeleteRun(ctx context.Context, runId string) error {
	res := bs.db.WithContext(ctx).Where("run_id = ?", runId).Unscoped().Delete(&models.WorkflowRun{})
	if res.Error != nil {
		bs.Error(ERR_RUN_DELETE, zap.Error(res.Error), zap.String("run-id", runId))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_RUN_DELETE)
	}

	return nil
}

func (bs *schedulerService) SearchRuns(ctx context.Context, searchBy *models.RunParams) ([]*models.WorkflowRun, error) {
	tx := bs.db.WithContext(ctx)

	var runs []*models.WorkflowRun

	if searchBy.Type != "" {
		tx = tx.Where("type = ?", searchBy.Type)
	}
	if searchBy.Status != "" {
		tx = tx.Where("status = ?", searchBy.Status)
	} else {
		tx = tx.Not("status = ?", string(models.COMPLETED))
	}
	if searchBy.RunId != "" {
		tx = tx.Where("run_id = ?", searchBy.RunId)
	}
	if searchBy.WorkflowId != "" {
		tx = tx.Where("workflow_id = ?", searchBy.WorkflowId)
	}
	if searchBy.ExternalRef != "" {
		tx = tx.Where("external_ref = ?", searchBy.ExternalRef)
	}

	res := tx.Find(&runs)
	if res.Error != nil {
		bs.Error(ERR_RUN_FETCH, zap.Error(res.Error))
		return nil, errors.WrapError(res.Error, ERR_RUN_FETCH)
	}
	return runs, nil
}
