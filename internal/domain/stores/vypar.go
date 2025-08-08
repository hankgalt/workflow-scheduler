package stores

import (
	"strconv"
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

type BusinessEntityType string

const (
	EntityTypeAgent  BusinessEntityType = "agent"
	EntityTypeFiling BusinessEntityType = "filing"
)

type Agent struct {
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

type Filing struct {
	EntityName             string            `bson:"entityName"`
	EntityID               uint64            `bson:"entityId"`
	InitialFilingDate      uint64            `bson:"initialFilingDate"`
	Jurisdiction           string            `bson:"jurisdiction"`
	EntityStatus           string            `bson:"entityStatus"`
	StandingSOS            string            `bson:"standingSos"`
	EntityType             string            `bson:"entityType"`
	FilingType             string            `bson:"filingType"`
	ForeignName            string            `bson:"foreignName"`
	StandingFTB            string            `bson:"standingFtb"`
	StandingVCFCF          string            `bson:"standingVcfcf"`
	SuspensionDate         uint64            `bson:"suspensionDate"`
	LastSIFileNumber       string            `bson:"lastSiFileNumber"`
	LastSIFileDate         uint64            `bson:"lastSiFileDate"`
	PrincipalAddress       string            `bson:"principalAddress"`
	MailingAddress         string            `bson:"mailingAddress"`
	PrincipalAddressInCA   string            `bson:"principalAddressInCa"`
	LLCManagementStructure string            `bson:"llcManagementStructure"`
	TypeOfBusiness         string            `bson:"typeOfBusiness"`
	CreatedAt              time.Time         `bson:"createdAt"`
	UpdatedAt              time.Time         `bson:"updatedAt"`
	Metadata               map[string]string `bson:"metadata,omitempty"` // additional metadata for the agent
}

func MapAgentFieldsToMongoModel(fields map[string]string) Agent {
	agent := Agent{}
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

func MapEntityTypeToModel(ty api.EntityType) BusinessEntityType {
	switch ty {
	case api.EntityType_AGENT:
		return EntityTypeAgent
	case api.EntityType_FILING:
		return EntityTypeFiling
	default:
		return ""
	}
}

func MapAgentModelToProto(agent *Agent, id string) *api.Agent {
	if agent == nil {
		return nil
	}

	return &api.Agent{
		Id:         id,
		EntityName: agent.EntityName,
		EntityId:   agent.EntityID,
		EntityOrg:  agent.OrgName,
		FirstName:  agent.FirstName,
		MiddleName: agent.MiddleName,
		LastName:   agent.LastName,
		Address:    agent.Address,
		AgentType:  agent.AgentType,
	}
}

func MapFilingFieldsToMongoModel(fields map[string]string) Filing {
	filing := Filing{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			filing.EntityName = v
		case "entity_num":
			num, err := strconv.Atoi(v)
			if err == nil {
				filing.EntityID = uint64(num)
			} else {
				filing.EntityID = 0 // Default to 0 if conversion fails
			}
		case "initial_filing_date":
			if date, err := strconv.ParseUint(v, 10, 64); err == nil {
				filing.InitialFilingDate = date
			}
		case "jurisdiction":
			filing.Jurisdiction = v
		case "filing_type":
			filing.FilingType = v
		case "foreign_name":
			filing.ForeignName = v
		case "suspension_date":
			if date, err := strconv.ParseUint(v, 10, 64); err == nil {
				filing.SuspensionDate = date
			}
		case "principal_address":
			filing.PrincipalAddress = v
		case "mailing_address":
			filing.MailingAddress = v
		}
	}

	return filing
}

func MapFilingModelToProto(filing *Filing, id string) *api.Filing {
	if filing == nil {
		return nil
	}

	return &api.Filing{
		Id:                id,
		EntityName:        filing.EntityName,
		EntityId:          filing.EntityID,
		InitialFilingDate: filing.InitialFilingDate,
		Jurisdiction:      filing.Jurisdiction,
		FilingType:        filing.FilingType,
		ForeignName:       filing.ForeignName,
		SuspensionDate:    filing.SuspensionDate,
		PrincipalAddress:  filing.PrincipalAddress,
		MailingAddress:    filing.MailingAddress,
	}
}
