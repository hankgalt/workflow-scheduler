package batch

import (
	"os"

	"github.com/google/uuid"
)

const ApplicationName = "processBatchTaskGroup"

// HostID - hostname or IP address
var host string

// init initializes the host variable with the hostname of the machine.
// If the hostname cannot be determined, it generates a new UUID as a fallback.
// host uniquely identifies a worker process host in distributed systems.
func init() {
	host, _ = os.Hostname()
	if host == "" {
		host = uuid.New().String()
	}
}

var HostID = host + "_" + ApplicationName

// ProcessLocalCSV is the task list / workflow for processing data from a local csv file source & sink.
const ProcessLocalCSVWorkflow = "github.com/hankgalt/workflow-scheduler/pkg/workflows/batch.ProcessLocalCSV"

// ProcessCloudCSV is the task list / workflow for processing data from a cloud csv file source & sink.
const ProcessCloudCSVWorkflow = "github.com/hankgalt/workflow-scheduler/pkg/workflows/batch.ProcessCloudCSV"

// ProcessLocalCSVMongo is the task list / workflow for processing data from a local csv file source & MongoDB sink.
const ProcessLocalCSVMongoWorkflow = "github.com/hankgalt/workflow-scheduler/pkg/workflows/batch.ProcessLocalCSVMongo"
