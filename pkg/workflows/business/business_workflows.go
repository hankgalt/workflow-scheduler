package business

import "github.com/google/uuid"

// ApplicationName is the task list for this workflow
const ApplicationName = "BusinessTaskGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// AddAgentSignalWorkflowName is the task list for this workflow
const AddAgentSignalWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/business.AddAgentSignalWorkflow"

// ProcessCSVWorkflowName is the task list for this workflow
const ProcessCSVWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/business.ProcessCSVWorkflow"

// ReadCSVWorkflowName is the task list for this workflow
const ReadCSVWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/business.ReadCSVWorkflow"

// ReadCSVRecordsWorkflowName is the task list for this workflow
const ReadCSVRecordsWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/business.ReadCSVRecordsWorkflow"
