package stores

import (
	"strconv"
	"time"
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
