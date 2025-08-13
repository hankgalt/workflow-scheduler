package stores

import (
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
)

type RunStatus string

const (
	UPLOADED          RunStatus = "UPLOADED"
	RUN_STARTED       RunStatus = "RUN_STARTED"
	DOWNLOADED        RunStatus = "DOWNLOADED"
	DRY_RUN_COMPLETED RunStatus = "DRY_RUN_COMPLETED"
	RUN_COMPLETED     RunStatus = "RUN_COMPLETED"
)

type WorkflowRun struct {
	RunId       string    `bson:"run_id"`
	WorkflowId  string    `bson:"workflow_id"`
	Status      string    `bson:"status"`
	Type        string    `bson:"type"`
	ExternalRef string    `bson:"external_ref"`
	CreatedBy   string    `bson:"created_by"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
	DeletedAt   time.Time `bson:"deleted_at"`
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

func MapWorkflowRunToProto(wkflRun *WorkflowRun) *api.WorkflowRun {
	return &api.WorkflowRun{
		RunId:       wkflRun.RunId,
		WorkflowId:  wkflRun.WorkflowId,
		Status:      wkflRun.Status,
		Type:        wkflRun.Type,
		ExternalRef: wkflRun.ExternalRef,
	}
}
