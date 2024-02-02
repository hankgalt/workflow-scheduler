package common

import "github.com/google/uuid"

// ApplicationName is the task list for this workflow
const ApplicationName = "commonTaskGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// CreateRunWorkflowName is the task list for create run workflow
const CreateRunWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common.CreateRunWorkflow"
