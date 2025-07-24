package models

import (
	"strconv"
	"time"

	"gorm.io/gorm"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

type BusinessFilingMongo struct {
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

type BusinessFilingSql struct {
	ID                     string `gorm:"primary_key;not null"`
	EntityName             string
	EntityID               uint64 `gorm:"primary_key;not null"`
	InitialFilingDate      uint64
	Jurisdiction           string
	EntityStatus           string
	StandingSOS            string
	EntityType             string
	FilingType             string
	ForeignName            string
	StandingFTB            string
	StandingVCFCF          string
	SuspensionDate         uint64
	LastSIFileNumber       string
	LastSIFileDate         uint64
	PrincipalAddress       string
	MailingAddress         string
	PrincipalAddressInCA   string
	LLCManagementStructure string
	TypeOfBusiness         string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	DeletedAt              gorm.DeletedAt `gorm:"index"`
}

func (bf *BusinessFilingSql) TableName() string {
	return "business_filings"
}

func (bf *BusinessFilingSql) BeforeCreate(tx *gorm.DB) (err error) {
	return
}

func (bf *BusinessFilingSql) AfterDelete(tx *gorm.DB) (err error) {
	return
}

func MapFilingFieldsToModel(fields map[string]string) *BusinessFilingSql {
	bf := BusinessFilingSql{}
	for k, v := range fields {
		switch k {
		case "entity_name":
			bf.EntityName = v
		case "entity_num":
			num, err := strconv.Atoi(v)
			if err == nil {
				bf.EntityID = uint64(num)
				bf.ID = v
			}
		case "initial_filing_date":
			ts, err := strconv.Atoi(v)
			if err == nil {
				bf.InitialFilingDate = uint64(ts)
			}
		case "jurisdiction":
			bf.Jurisdiction = v
		case "entity_status":
			bf.EntityStatus = v
		case "standing_sos":
			bf.StandingSOS = v
		case "entity_type":
			bf.EntityType = v
		case "filing_type":
			bf.FilingType = v
		case "foreign_name":
			bf.ForeignName = v
		case "standing_ftb":
			bf.StandingFTB = v
		case "standing_vcfcf":
			bf.StandingVCFCF = v
		case "suspension_date":
			ts, err := strconv.Atoi(v)
			if err == nil {
				bf.SuspensionDate = uint64(ts)
			}
		case "last_si_file_number":
			bf.LastSIFileNumber = v
		case "last_si_file_date":
			ts, err := strconv.Atoi(v)
			if err == nil {
				bf.LastSIFileDate = uint64(ts)
			}
		case "principal_address":
			bf.PrincipalAddress = v
		case "mailing_address":
			bf.MailingAddress = v
		case "principal_address_in_ca":
			bf.PrincipalAddressInCA = v
		case "llc_management_structure":
			bf.LLCManagementStructure = v
		case "type_of_business":
			bf.TypeOfBusiness = v
		}
	}
	return &bf
}

func MapFilingModelToProto(bf *BusinessFilingSql) *api.BusinessFiling {
	return &api.BusinessFiling{
		Id:                  bf.ID,
		EntityId:            uint64(bf.EntityID),
		Name:                bf.EntityName,
		InitialFilingDate:   bf.InitialFilingDate,
		Jurisdiction:        bf.Jurisdiction,
		Status:              bf.EntityStatus,
		Type:                bf.EntityType,
		FilingType:          bf.FilingType,
		ForeignName:         bf.ForeignName,
		Sos:                 bf.StandingSOS,
		Ftb:                 bf.StandingFTB,
		Vcfcf:               bf.StandingVCFCF,
		SuspensionDate:      bf.SuspensionDate,
		LastFiledNum:        bf.LastSIFileNumber,
		LastFiledDate:       bf.LastSIFileDate,
		PrincipalAddress:    bf.PrincipalAddress,
		MailingAddress:      bf.MailingAddress,
		LocalAddress:        bf.PrincipalAddressInCA,
		ManagementStructure: bf.LLCManagementStructure,
		BusinessType:        bf.TypeOfBusiness,
	}
}
