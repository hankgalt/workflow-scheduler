package models

import (
	"log"

	"gorm.io/gorm"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
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
	RunId       string `gorm:"primary_key;not null" json:"run_id"`
	WorkflowId  string `gorm:"primary_key;not null" json:"workflow_id"`
	Status      string
	Type        string
	ExternalRef string
	CreatedBy   string
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
	RunId       string
	WorkflowId  string
	RequestedBy string
	FilePath    string
	Status      string
	Type        string
	ExternalRef string
}

type RequestInfo struct {
	FileName    string
	HostID      string
	RequestedBy string
	RunId       string
	WorkflowId  string
	Status      string
}

func MapToRunProto(wkfl *WorkflowRun) *api.WorkflowRun {
	if wkfl == nil {
		return nil
	}
	return &api.WorkflowRun{
		RunId:       wkfl.RunId,
		WorkflowId:  wkfl.WorkflowId,
		Status:      wkfl.Status,
		Type:        wkfl.Type,
		ExternalRef: wkfl.ExternalRef,
		RequestedBy: wkfl.CreatedBy,
	}
}

func MapToRunProtos(wfs []*WorkflowRun) []*api.WorkflowRun {
	if wfs == nil || len(wfs) < 1 {
		return nil
	}
	wkfls := []*api.WorkflowRun{}
	for _, wf := range wfs {
		wkfls = append(wkfls, MapToRunProto(wf))
	}
	return wkfls
}

func MapToRunModel(wkfl *api.WorkflowRun) *WorkflowRun {
	if wkfl == nil {
		return nil
	}
	return &WorkflowRun{
		RunId:       wkfl.RunId,
		WorkflowId:  wkfl.WorkflowId,
		Status:      wkfl.Status,
		Type:        wkfl.Type,
		ExternalRef: wkfl.ExternalRef,
	}
}

func MapToRunModels(wfs []*api.WorkflowRun) []*WorkflowRun {
	if wfs == nil || len(wfs) < 1 {
		return nil
	}
	wkfls := []*WorkflowRun{}
	for _, wf := range wfs {
		wkfls = append(wkfls, MapToRunModel(wf))
	}
	return wkfls
}
