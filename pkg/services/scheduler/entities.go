package scheduler

import (
	"context"

	"github.com/comfforts/errors"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/zap"
)

const (
	ERR_AGENT_ADD           = "error: adding business agent"
	ERR_AGENT_DELETE        = "error: deleting business agent"
	ERR_AGENT_GET           = "error: getting business agent"
	ERR_PRINCIPAL_ADD       = "error: adding business principal"
	ERR_PRINCIPAL_DELETE    = "error: deleting business principal"
	ERR_PRINCIPAL_GET       = "error: getting business principal"
	ERR_FILING_ADD          = "error: adding business filing"
	ERR_FILING_DELETE       = "error: deleting business filing"
	ERR_FILING_GET          = "error: getting business filing"
	ERR_DUPLICATE_AGENT     = "error: duplicate agent"
	ERR_DUPLICATE_PRINCIPAL = "error: duplicate principal"
	ERR_DUPLICATE_FILING    = "error: duplicate filing"
)

var (
	ErrDuplicateAgent     = errors.NewAppError(ERR_DUPLICATE_AGENT)
	ErrDuplicatePrincipal = errors.NewAppError(ERR_DUPLICATE_PRINCIPAL)
	ErrDuplicateFiling    = errors.NewAppError(ERR_DUPLICATE_FILING)
)

func (bs *schedulerService) AddAgent(ctx context.Context, ba *models.BusinessAgentSql) (*models.BusinessAgentSql, error) {
	res := bs.db.WithContext(ctx).Create(ba)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				return nil, ErrDuplicateAgent
			}
		}

		return nil, errors.WrapError(res.Error, ERR_AGENT_ADD)
	}
	return ba, nil
}

func (bs *schedulerService) DeleteAgent(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessAgentSql{})
	if res.Error != nil {
		bs.Error(ERR_AGENT_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_AGENT_DELETE)
	}
	return nil
}

func (bs *schedulerService) GetAgent(ctx context.Context, id string) (*models.BusinessAgentSql, error) {
	var agent models.BusinessAgentSql
	res := bs.db.WithContext(ctx).Where("id = ?", id).First(&agent)
	if res.Error != nil {
		bs.Error(ERR_AGENT_GET, zap.Error(res.Error), zap.String("id", id))
		return nil, errors.WrapError(res.Error, ERR_AGENT_GET)
	}
	return &agent, nil
}

func (bs *schedulerService) AddPrincipal(ctx context.Context, bp *models.BusinessPrincipalSql) (*models.BusinessPrincipalSql, error) {
	res := bs.db.WithContext(ctx).Create(bp)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				return nil, ErrDuplicatePrincipal
			}
		}
		return nil, errors.WrapError(res.Error, ERR_PRINCIPAL_ADD)
	}
	return bp, nil
}

func (bs *schedulerService) DeletePrincipal(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessPrincipalSql{})
	if res.Error != nil {
		bs.Error(ERR_PRINCIPAL_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_PRINCIPAL_DELETE)
	}
	return nil
}

func (bs *schedulerService) GetPrincipal(ctx context.Context, id string) (*models.BusinessPrincipalSql, error) {
	var pri models.BusinessPrincipalSql
	res := bs.db.WithContext(ctx).Where("id = ?", id).First(&pri)
	if res.Error != nil {
		bs.Error(ERR_PRINCIPAL_GET, zap.Error(res.Error), zap.String("id", id))
		return nil, errors.WrapError(res.Error, ERR_PRINCIPAL_GET)
	}
	return &pri, nil
}

func (bs *schedulerService) AddFiling(ctx context.Context, bf *models.BusinessFilingSql) (*models.BusinessFilingSql, error) {
	res := bs.db.WithContext(ctx).Create(bf)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				return nil, ErrDuplicateFiling
			}
		}
		return nil, errors.WrapError(res.Error, ERR_FILING_ADD)
	}
	return bf, nil
}

func (bs *schedulerService) DeleteFiling(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessFilingSql{})
	if res.Error != nil {
		bs.Error(ERR_FILING_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_FILING_DELETE)
	}
	return nil
}

func (bs *schedulerService) GetFiling(ctx context.Context, id string) (*models.BusinessFilingSql, error) {
	var fil models.BusinessFilingSql
	res := bs.db.WithContext(ctx).Where("id = ?", id).First(&fil)
	if res.Error != nil {
		bs.Error(ERR_FILING_GET, zap.Error(res.Error), zap.String("id", id))
		return nil, errors.WrapError(res.Error, ERR_FILING_GET)
	}
	return &fil, nil
}
