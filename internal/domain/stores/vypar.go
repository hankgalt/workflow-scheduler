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
	EntityName string            `bson:"entity_name"`
	EntityID   uint64            `bson:"entity_id"`
	OrgName    string            `bson:"org_name"`
	FirstName  string            `bson:"first_name"`
	MiddleName string            `bson:"middle_name,omitempty"`
	LastName   string            `bson:"last_name"`
	Address    string            `bson:"address"`
	AgentType  string            `bson:"agent_type"`
	CreatedAt  time.Time         `bson:"created_at"`
	UpdatedAt  time.Time         `bson:"updated_at"`
	Metadata   map[string]string `bson:"metadata,omitempty"` // additional metadata for the agent
}

type Filing struct {
	EntityName             string            `bson:"entity_name"`
	EntityID               uint64            `bson:"entity_id"`
	InitialFilingDate      uint64            `bson:"initial_filing_date"`
	Jurisdiction           string            `bson:"jurisdiction"`
	EntityStatus           string            `bson:"entity_status"`
	StandingSOS            string            `bson:"standing_sos"`
	EntityType             string            `bson:"entity_type"`
	FilingType             string            `bson:"filing_type"`
	ForeignName            string            `bson:"foreign_name"`
	StandingFTB            string            `bson:"standing_ftb"`
	StandingVCFCF          string            `bson:"standing_vcfcf"`
	SuspensionDate         uint64            `bson:"suspension_date"`
	LastSIFileNumber       string            `bson:"last_si_file_number"`
	LastSIFileDate         uint64            `bson:"last_si_file_date"`
	PrincipalAddress       string            `bson:"principal_address"`
	MailingAddress         string            `bson:"mailing_address"`
	PrincipalAddressInCA   string            `bson:"principal_address_in_ca"`
	LLCManagementStructure string            `bson:"llc_management_structure"`
	TypeOfBusiness         string            `bson:"type_of_business"`
	CreatedAt              time.Time         `bson:"created_at"`
	UpdatedAt              time.Time         `bson:"updated_at"`
	Metadata               map[string]string `bson:"metadata,omitempty"` // additional metadata for the agent
}

func MapAgentFieldsToMongoModel(fields map[string]string) Agent {
	var entityId uint64
	if id, ok := fields["entity_num"]; ok {
		num, err := strconv.Atoi(id)
		if err == nil {
			entityId = uint64(num)
		} else {
			entityId = 0 // Default to 0 if conversion fails
		}
	}
	var entityName string
	if name, ok := fields["entity_name"]; ok {
		entityName = name
	} else {
		entityName = "Unknown Entity"
	}
	var orgName string
	if name, ok := fields["org_name"]; ok {
		orgName = name
	} else {
		orgName = "Unknown Organization"
	}
	var firstName, middleName, lastName string
	if name, ok := fields["first_name"]; ok {
		firstName = name
	}
	if name, ok := fields["middle_name"]; ok {
		middleName = name
	}
	if name, ok := fields["last_name"]; ok {
		lastName = name
	}
	var agentType string
	if at, ok := fields["agent_type"]; ok {
		agentType = at
	} else {
		agentType = "Unknown Type"
	}
	address := fields["physical_address1"] + " " + fields["physical_city"] + " " + fields["physical_state"] + " " + fields["physical_postal_code"] + " " + fields["physical_country"]

	if entityId == 0 || entityName == "" || firstName == "" {

	}

	return Agent{
		EntityID:   entityId,
		EntityName: entityName,
		OrgName:    orgName,
		FirstName:  firstName,
		MiddleName: middleName,
		LastName:   lastName,
		Address:    address,
		AgentType:  agentType,
	}
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
