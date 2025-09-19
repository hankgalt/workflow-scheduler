package batch

import (
	"os"

	"github.com/google/uuid"

	"github.com/hankgalt/batch-orchestra/pkg/domain"

	bsinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/snapshotters"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
)

const ApplicationName = "processBatchTaskGroup"

const (
	ProcessLocalCSVMongoNoopWorkflowAlias  string = "process-local-csv-mongo-noop-workflow-alias"
	ProcessCloudCSVMongoNoopWorkflowAlias  string = "process-cloud-csv-mongo-noop-workflow-alias"
	ProcessLocalCSVMongoLocalWorkflowAlias string = "process-local-csv-mongo-local-workflow-alias"
	ProcessCloudCSVMongoLocalWorkflowAlias string = "process-cloud-csv-mongo-local-workflow-alias"
	ProcessLocalCSVMongoCloudWorkflowAlias string = "process-local-csv-mongo-cloud-workflow-alias"
	ProcessCloudCSVMongoCloudWorkflowAlias string = "process-cloud-csv-mongo-cloud-workflow-alias"
)

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

type LocalCSVMongoNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.NoopSnapshotterConfig]
type LocalCSVNoopNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.NoopSnapshotterConfig]
type CloudCSVMongoNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.NoopSnapshotterConfig]
type CloudCSVNoopNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.NoopSnapshotterConfig]

type LocalCSVMongoLocalBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig]
type LocalCSVNoopLocalBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig]
type CloudCSVMongoLocalBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig]
type CloudCSVNoopLocalBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig]

type LocalCSVMongoCloudBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig]
type LocalCSVNoopCloudBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.LocalCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig]
type CloudCSVMongoCloudBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.MongoSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig]
type CloudCSVNoopCloudBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *sources.CloudCSVConfig, *bsinks.NoopSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig]
