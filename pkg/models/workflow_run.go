package models

import (
	"log"

	"gorm.io/gorm"
)

type StoreUploadRunStatus string

const (
	UPLOADED          StoreUploadRunStatus = "UPLOADED"
	STARTED           StoreUploadRunStatus = "STARTED"
	DOWNLOADED        StoreUploadRunStatus = "DOWNLOADED"
	DRY_RUN_COMPLETED StoreUploadRunStatus = "DRY_RUN_COMPLETED"
	COMPLETED         StoreUploadRunStatus = "COMPLETED"
)

type WorkflowRun struct {
	gorm.Model
	RunId      string `gorm:"primary_key;not null" json:"run_id"`
	WorkflowId string `gorm:"primary_key;not null" json:"workflow_id"`
	Status     string
	CreatedBy  string
}

func (wr *WorkflowRun) TableName() string {
	return "workflow_runs"
}

func (wr *WorkflowRun) BeforeCreate(tx *gorm.DB) (err error) {
	log.Printf("BeforeCreate() WorkflowRun - %v", wr)
	return
}

func (wr *WorkflowRun) AfterDelete(tx *gorm.DB) (err error) {
	log.Printf("AfterDelete() WorkflowRun - %v", wr)
	return
}

type RunParams struct {
	RequestedBy string
	FilePath    string
	Org         string
}

type RunUpdateParams struct {
	RunId       string
	WorkflowId  string
	Status      string
	RequestedBy string
}

type RequestInfo struct {
	FileName    string
	HostID      string
	RequestedBy string
	Org         string
	RunId       string
	WorkflowId  string
	Status      string
	Count       int
	Processed   int
}
