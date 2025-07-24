package models

import (
	"strconv"
	"time"

	"gorm.io/gorm"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

type EntityType string

const (
	AGENT     EntityType = "AGENT"
	PRINCIPAL EntityType = "PRINCIPAL"
	FILING    EntityType = "FILING"
	UNKNOWN   EntityType = "UNKNOWN"
)

type BusinessAgentMongo struct {
	EntityName string            `bson:"entityName"`
	EntityID   uint64            `bson:"entityId"`
	OrgName    string            `bson:"orgName"`
	FirstName  string            `bson:"firstName"`
	MiddleName string            `bson:"middleName,omitempty"`
	LastName   string            `bson:"lastName"`
	Address    string            `bson:"address"`
	AgentType  string            `bson:"agentType"`
	CreatedAt  time.Time         `bson:"createdAt"`
	UpdatedAt  time.Time         `bson:"updatedAt"`
	Metadata   map[string]string `bson:"metadata,omitempty"` // additional metadata for the agent
}

type BusinessAgentSql struct {
	ID              string `gorm:"primary_key;not null"`
	EntityName      string
	EntityID        uint64 `gorm:"primary_key;not null"`
	OrgName         string
	FirstName       string `gorm:"primary_key;not null"`
	MiddleName      string
	LastName        string `gorm:"primary_key;not null"`
	PhysicalAddress string
	AgentType       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
}

func (ba *BusinessAgentSql) TableName() string {
	return "business_agents"
}

func (ba *BusinessAgentSql) BeforeCreate(tx *gorm.DB) (err error) {
	return nil
}

func (ba *BusinessAgentSql) AfterDelete(tx *gorm.DB) (err error) {
	return nil
}

func MapAgentFieldsToModel(fields map[string]string) *BusinessAgentSql {
	agent := BusinessAgentSql{}
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

func MapAgentModelToProto(ag *BusinessAgentSql) *api.BusinessAgent {
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

func MapAgentFieldsToMongoModel(fields map[string]string) BusinessAgentMongo {
	agent := BusinessAgentMongo{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			agent.EntityName = v
		case "entity_num":
			num, err := strconv.Atoi(v)
			if err == nil {
				agent.EntityID = uint64(num)
			} else {
				agent.EntityID = 0 // Default to 0 if conversion fails
			}
		case "org_name":
			agent.OrgName = v
		case "first_name":
			agent.FirstName = v
		case "middle_name":
			agent.MiddleName = v
		case "last_name":
			agent.LastName = v
		case "agent_type":
			agent.AgentType = v
		}
	}

	agent.Address = fields["physical_address1"] + " " + fields["physical_city"] + " " + fields["physical_state"] + " " + fields["physical_postal_code"] + " " + fields["physical_country"]
	return agent
}

func MapEntityTypeToProto(et EntityType) api.EntityType {
	switch et {
	case AGENT:
		return 0
	case PRINCIPAL:
		return 1
	case FILING:
		return 2
	default:
		return 3
	}
}

func MapProtoToEntityType(et api.EntityType) EntityType {
	switch et {
	case api.EntityType_AGENT:
		return AGENT
	case api.EntityType_PRINCIPAL:
		return PRINCIPAL
	case api.EntityType_FILING:
		return FILING
	default:
		return UNKNOWN
	}
}
