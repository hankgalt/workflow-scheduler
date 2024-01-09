package models

import (
	"strconv"
	"time"

	"gorm.io/gorm"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

type BusinessAgent struct {
	ID              string `gorm:"primary_key;not null"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
	EntityName      string
	EntityID        uint64 `gorm:"primary_key;not null"`
	OrgName         string
	FirstName       string `gorm:"primary_key;not null"`
	MiddleName      string
	LastName        string `gorm:"primary_key;not null"`
	PhysicalAddress string
	AgentType       string
}

func (ba *BusinessAgent) TableName() string {
	return "business_agents"
}

func (ba *BusinessAgent) BeforeCreate(tx *gorm.DB) (err error) {
	return nil
}

func (ba *BusinessAgent) AfterDelete(tx *gorm.DB) (err error) {
	return nil
}

func MapAgentFieldsToModel(fields map[string]string) *BusinessAgent {
	agent := BusinessAgent{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			agent.EntityName = v
		case "entity_num":
			num, err := strconv.Atoi(v)
			if err == nil {
				agent.EntityID = uint64(num)
				agent.ID = v
			}
		case "org_name":
			agent.OrgName = v
		case "first_name":
			agent.FirstName = v
		case "middle_name":
			agent.MiddleName = v
		case "last_name":
			agent.LastName = v
		case "physical_address":
			agent.PhysicalAddress = v
		case "agent_type":
			agent.AgentType = v
		}
	}
	return &agent
}

func MapAgentModelToProto(ag *BusinessAgent) *api.BusinessAgent {
	return &api.BusinessAgent{
		Id:         ag.ID,
		EntityId:   uint64(ag.EntityID),
		Name:       ag.EntityName,
		Org:        ag.OrgName,
		FirstName:  ag.FirstName,
		MiddleName: ag.MiddleName,
		LastName:   ag.LastName,
		Address:    ag.PhysicalAddress,
		AgentType:  ag.AgentType,
	}
}
