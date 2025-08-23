package stores

import (
	"strconv"
	"strings"
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/business/v1"
)

type BusinessEntityType string

const (
	EntityTypeAgent  BusinessEntityType = "agent"
	EntityTypeFiling BusinessEntityType = "filing"
)

type Agent struct {
	EntityName   string            `bson:"entity_name"`
	EntityID     string            `bson:"entity_id"`
	OrgName      string            `bson:"org_name"`
	Name         string            `bson:"name"`
	Address      string            `bson:"address"`
	AgentType    string            `bson:"agent_type"`
	PositionType string            `bson:"position_type,omitempty"`
	CreatedAt    time.Time         `bson:"created_at"`
	UpdatedAt    time.Time         `bson:"updated_at"`
	Metadata     map[string]string `bson:"metadata,omitempty"` // additional metadata for the agent
}

type Filing struct {
	EntityName             string            `bson:"entity_name"`
	EntityID               string            `bson:"entity_id"`
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
	var entityId string
	if id, ok := fields["entity_num"]; ok {
		entityId = id
	}
	var entityName string
	if name, ok := fields["entity_name"]; ok {
		entityName = name
	}

	var orgName string
	if name, ok := fields["org_name"]; ok {
		orgName = name
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

	name := firstName
	for _, v := range []string{middleName, lastName} {
		if v != "" {
			name += " " + v
		}
	}
	name = strings.TrimSpace(name)

	var agentType string
	if at, ok := fields["agent_type"]; ok {
		agentType = at
	}

	ag := Agent{
		EntityID:   entityId,
		EntityName: entityName,
		OrgName:    orgName,
		Name:       name,
		AgentType:  agentType,
	}

	if pt, ok := fields["position_type"]; ok {
		ag.PositionType = pt
		ag.AgentType = "Principal"
		ag.Address = BuildAddress(fields, "principal")
	} else {
		ag.Address = BuildAddress(fields, "agent")
	}

	return ag
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
		Id:           id,
		EntityName:   agent.EntityName,
		EntityId:     agent.EntityID,
		EntityOrg:    agent.OrgName,
		Name:         agent.Name,
		Address:      agent.Address,
		AgentType:    agent.AgentType,
		PositionType: agent.PositionType,
	}
}

func MapFilingFieldsToMongoModel(fields map[string]string) Filing {
	filing := Filing{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			filing.EntityName = v
		case "entity_num":
			filing.EntityID = v
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

func BuildAddress(fields map[string]string, addrType string) string {
	var address string
	if addrType == "agent" {
		var addr1, addr2, addr3, city, state, postalCode, country string
		if a1, ok := fields["physical_address1"]; ok {
			addr1 = a1
		}
		if a2, ok := fields["physical_address2"]; ok {
			addr2 = a2
		}
		if a3, ok := fields["physical_address3"]; ok {
			addr3 = a3
		}
		if c, ok := fields["physical_city"]; ok {
			city = c
		}
		if s, ok := fields["physical_state"]; ok {
			state = s
		}
		if zip, ok := fields["physical_postal_code"]; ok {
			postalCode = zip
		}
		if cntry, ok := fields["physical_country"]; ok {
			country = cntry
		}

		address = addr1
		for _, v := range []string{addr2, addr3, city, state, postalCode, country} {
			if v != "" {
				address += " " + v
			}
		}
	} else {
		var addr1, addr2, addr3, city, state, postalCode, country string
		if a1, ok := fields["address1"]; ok {
			addr1 = a1
		}
		if a2, ok := fields["address2"]; ok {
			addr2 = a2
		}
		if a3, ok := fields["address3"]; ok {
			addr3 = a3
		}
		if c, ok := fields["city"]; ok {
			city = c
		}
		if s, ok := fields["state"]; ok {
			state = s
		}
		if zip, ok := fields["postal_code"]; ok {
			postalCode = zip
		}
		if cntry, ok := fields["country"]; ok {
			country = cntry
		}

		address = addr1
		for _, v := range []string{addr2, addr3, city, state, postalCode, country} {
			if v != "" {
				address += " " + v
			}
		}
	}

	address = strings.TrimSpace(address)
	return address
}
