package file

import (
	"github.com/google/uuid"
)

// ApplicationName is the task list for this workflow
const ApplicationName = "FileProcessingGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// UploadFileWorkflowName is the task list for file upload workflow
const UploadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.UploadFileWorkflow"

// DownloadFileWorkflowName is the task list for file download workflow
const DownloadFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DownloadFileWorkflow"

// DeleteFileWorkflowName is the task list for delete file workflow
const DeleteFileWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DeleteFileWorkflow"
