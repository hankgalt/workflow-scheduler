package models

import (
	"strconv"
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"gorm.io/gorm"
)

type BusinessPrincipalSql struct {
	ID           string `gorm:"primary_key;not null"`
	EntityName   string
	EntityID     uint64 `gorm:"primary_key;not null"`
	OrgName      string
	FirstName    string `gorm:"primary_key;not null"`
	MiddleName   string
	LastName     string `gorm:"primary_key;not null"`
	Address      string
	PositionType string `gorm:"primary_key;not null"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    gorm.DeletedAt `gorm:"index"`
}

func (bp *BusinessPrincipalSql) TableName() string {
	return "business_principals"
}

func (bp *BusinessPrincipalSql) BeforeCreate(tx *gorm.DB) (err error) {
	return
}

func (bp *BusinessPrincipalSql) AfterDelete(tx *gorm.DB) (err error) {
	return
}

func MapPrincipalFieldsToModel(fields map[string]string) *BusinessPrincipalSql {
	bp := BusinessPrincipalSql{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			bp.EntityName = v
		case "entity_num":
			num, err := strconv.Atoi(v)
			if err == nil {
				bp.EntityID = uint64(num)
				bp.ID = v
			}
		case "org_name":
			bp.OrgName = v
		case "first_name":
			bp.FirstName = v
		case "middle_name":
			bp.MiddleName = v
		case "last_name":
			bp.LastName = v
		case "address":
			bp.Address = v
		case "position_type":
			bp.PositionType = v
		}
	}
	return &bp
}

func MapPrincipalModelToProto(bp *BusinessPrincipalSql) *api.BusinessPrincipal {
	return &api.BusinessPrincipal{
		Id:           bp.ID,
		EntityId:     uint64(bp.EntityID),
		Name:         bp.EntityName,
		Org:          bp.OrgName,
		FirstName:    bp.FirstName,
		MiddleName:   bp.MiddleName,
		LastName:     bp.LastName,
		Address:      bp.Address,
		PositionType: bp.PositionType,
	}
}
