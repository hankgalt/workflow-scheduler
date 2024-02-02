package business

import "github.com/google/uuid"

// ApplicationName is the task list for this workflow
const ApplicationName = "businessTaskGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// AddAgentSignalWorkflowName is the task list for add agent workflow
const AddAgentSignalWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business.AddAgentSignalWorkflow"

// ProcessCSVWorkflowName is the task list for process CSV workflow
const ProcessCSVWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business.ProcessCSVWorkflow"

// ReadCSVWorkflowName is the task list for CSV read workflow
const ReadCSVWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business.ReadCSVWorkflow"

// ReadCSVRecordsWorkflowName is the task list for CSV record read workflow
const ReadCSVRecordsWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business.ReadCSVRecordsWorkflow"

// ProcessFileSignalWorkflowName is the task list for file signal processing workflow
const ProcessFileSignalWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business.ProcessFileSignalWorkflow"
