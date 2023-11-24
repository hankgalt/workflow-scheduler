package scheduler

import (
	"context"
	"encoding/base64"
	"math"

	"github.com/comfforts/errors"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/zap"
)

const (
	ERR_AGENT_ADD           = "error: adding business agent"
	ERR_AGENT_DELETE        = "error: deleting business agent"
	ERR_PRINCIPAL_ADD       = "error: adding business principal"
	ERR_PRINCIPAL_DELETE    = "error: deleting business principal"
	ERR_FILING_ADD          = "error: adding business filing"
	ERR_FILING_DELETE       = "error: deleting business filing"
	ERR_DUPLICATE_AGENT     = "error: duplicate agent"
	ERR_DUPLICATE_PRINCIPAL = "error: duplicate principal"
	ERR_DUPLICATE_FILING    = "error: duplicate filing"
)

var (
	ErrDuplicateAgent     = errors.NewAppError(ERR_DUPLICATE_AGENT)
	ErrDuplicatePrincipal = errors.NewAppError(ERR_DUPLICATE_PRINCIPAL)
	ErrDuplicateFiling    = errors.NewAppError(ERR_DUPLICATE_FILING)
)

func (bs *schedulerService) AddAgent(ctx context.Context, ba *models.BusinessAgent) (*models.BusinessAgent, error) {
	res := bs.db.WithContext(ctx).Create(ba)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				ba.ID = encodeAgentEntityId(ctx, ba)
				res = bs.db.WithContext(ctx).Create(ba)

				if res.Error != nil {
					if driverErr, ok = res.Error.(*gomysql.MySQLError); ok {
						if driverErr.Number == 1062 {
							return nil, ErrDuplicateAgent
						}
					}
				} else {
					return ba, nil
				}
			}
		}

		return nil, errors.WrapError(res.Error, ERR_AGENT_ADD)
	}
	return ba, nil
}

func encodeAgentEntityId(ctx context.Context, ba *models.BusinessAgent) string {
	tag := ""
	if ba.FirstName != "" {
		tag = tag + ba.FirstName
	}

	if ba.AgentType != "" {
		tag = tag + ba.AgentType
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(tag))
	return ba.ID + encoded
}

func (bs *schedulerService) DeleteAgent(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessAgent{})
	if res.Error != nil {
		bs.Error(ERR_AGENT_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_AGENT_DELETE)
	}
	return nil
}

func (bs *schedulerService) AddPrincipal(ctx context.Context, bp *models.BusinessPrincipal) (*models.BusinessPrincipal, error) {
	res := bs.db.WithContext(ctx).Create(bp)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				bp.ID = encodePrincipalEntityId(ctx, bp)
				res := bs.db.WithContext(ctx).Create(bp)

				if res.Error != nil {
					if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
						if driverErr.Number == 1062 {
							return nil, ErrDuplicatePrincipal
						}
					}
				} else {
					return bp, nil
				}
			}
		}
		return nil, errors.WrapError(res.Error, ERR_PRINCIPAL_ADD)
	}
	return bp, nil
}

func encodePrincipalEntityId(ctx context.Context, bp *models.BusinessPrincipal) string {
	tag := ""
	if bp.FirstName != "" {
		tag = tag + bp.FirstName
	}

	if bp.PositionType != "" {
		tag = tag + bp.PositionType
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(tag))
	return bp.ID + encoded
}

func (bs *schedulerService) DeletePrincipal(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessPrincipal{})
	if res.Error != nil {
		bs.Error(ERR_PRINCIPAL_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_PRINCIPAL_DELETE)
	}
	return nil
}

func (bs *schedulerService) AddFiling(ctx context.Context, bf *models.BusinessFiling) (*models.BusinessFiling, error) {
	res := bs.db.WithContext(ctx).Create(bf)
	if res.Error != nil {
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			if driverErr.Number == 1062 {
				bf.ID = encodeFilingEntityId(ctx, bf)
				res = bs.db.WithContext(ctx).Create(bf)

				if res.Error != nil {
					if driverErr, ok = res.Error.(*gomysql.MySQLError); ok {
						if driverErr.Number == 1062 {
							return nil, ErrDuplicateFiling
						}
					}
				} else {
					return bf, nil
				}
			}
		}
		return nil, errors.WrapError(res.Error, ERR_FILING_ADD)
	}
	return bf, nil
}

func encodeFilingEntityId(ctx context.Context, bf *models.BusinessFiling) string {
	tag := ""
	if bf.EntityName != "" {
		n := math.Min(float64(len(bf.EntityName)), 10)
		tag = tag + bf.EntityName[:int(n)]
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(tag))
	return bf.ID + encoded
}

func (bs *schedulerService) DeleteFiling(ctx context.Context, id string) error {
	res := bs.db.WithContext(ctx).Where("id = ?", id).Unscoped().Delete(&models.BusinessFiling{})
	if res.Error != nil {
		bs.Error(ERR_FILING_DELETE, zap.Error(res.Error), zap.String("id", id))
		if driverErr, ok := res.Error.(*gomysql.MySQLError); ok {
			bs.Debug("mysql error", zap.Uint16("err-num", driverErr.Number), zap.String("err-msg", driverErr.Message))
		}
		return errors.WrapError(res.Error, ERR_FILING_DELETE)
	}
	return nil
}
