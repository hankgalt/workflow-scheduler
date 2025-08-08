package stores

import (
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
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
	RunId       string    `bson:"runId"`
	WorkflowId  string    `bson:"workflowId"`
	Status      string    `bson:"status"`
	Type        string    `bson:"type"`
	ExternalRef string    `bson:"externalRef"`
	CreatedBy   string    `bson:"createdBy"`
	CreatedAt   time.Time `bson:"createdAt"`
	UpdatedAt   time.Time `bson:"updatedAt"`
	DeletedAt   time.Time `bson:"deletedAt"`
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
