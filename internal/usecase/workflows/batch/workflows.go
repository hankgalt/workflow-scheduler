package batch

import (
	"os"

	"github.com/google/uuid"

	"github.com/hankgalt/batch-orchestra/pkg/domain"

	bsinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
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

const (
	ProcessLocalCSVMongoWorkflowAlias string = "process-local-csv-mongo-workflow-alias"
	ProcessCloudCSVMongoWorkflowAlias string = "process-cloud-csv-mongo-workflow-alias"
	ProcessLocalCSVNoopWorkflowAlias  string = "process-local-csv-noop-workflow-alias"
	ProcessCloudCSVNoopWorkflowAlias  string = "process-cloud-csv-noop-workflow-alias"
	FetchNextLocalCSVSourceBatchAlias string = "fetch-next-" + sources.LocalCSVSource + "-batch-alias"
	FetchNextCloudCSVSourceBatchAlias string = "fetch-next-" + sources.CloudCSVSource + "-batch-alias"
	WriteNextNoopSinkBatchAlias       string = "write-next-" + bsinks.NoopSink + "-batch-alias"
	WriteNextMongoSinkBatchAlias      string = "write-next-" + bsinks.MongoSink + "-batch-alias"
)

type LocalCSVMongoBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, bsinks.MongoSinkConfig[domain.CSVRow]]
type LocalCSVNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, bsinks.NoopSinkConfig[domain.CSVRow]]
type CloudCSVMongoBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.CloudCSVConfig, bsinks.MongoSinkConfig[domain.CSVRow]]
type CloudCSVNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.CloudCSVConfig, bsinks.NoopSinkConfig[domain.CSVRow]]
