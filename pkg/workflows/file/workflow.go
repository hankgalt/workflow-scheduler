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
const UploadFileWorkflowName string = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.UploadFileWorkflow"

// DownloadFileWorkflowName is the task list for file download workflow
const DownloadFileWorkflowName string = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DownloadFileWorkflow"

// DeleteFileWorkflowName is the task list for delete file workflow
const DeleteFileWorkflowName string = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.DeleteFileWorkflow"

// FileSignalWorkflowName is the task list for file signal workflow
const FileSignalWorkflowName string = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.FileSignalWorkflow"
