package batch

import (
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/snapshotters"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
)

const (
	FetchNextLocalCSVSourceBatchActivityAlias  string = "fetch-next-" + sources.LocalCSVSource + "-batch-activity-alias"
	FetchNextCloudCSVSourceBatchActivityAlias  string = "fetch-next-" + sources.CloudCSVSource + "-batch-activity-alias"
	FetchNextLocalJSONSourceBatchActivityAlias string = "fetch-next-" + sources.LocalJSONSource + "-batch-activity-alias"
	WriteNextNoopSinkBatchActivityAlias        string = "write-next-" + sinks.NoopSink + "-batch-activity-alias"
	WriteNextMongoSinkBatchActivityAlias       string = "write-next-" + sinks.MongoSink + "-batch-activity-alias"
	WriteNextLocalJSONSinkBatchActivityAlias   string = "write-next-" + sinks.LocalJSONSink + "-batch-activity-alias"
	SnapshotLocalBatchActivityAlias            string = "snapshot-" + snapshotters.LocalSnapshotter + "-batch-activity-alias"
	SnapshotCloudBatchActivityAlias            string = "snapshot-" + snapshotters.CloudSnapshotter + "-batch-activity-alias"
	SnapshotNoopBatchActivityAlias             string = "snapshot-" + snapshotters.NoopSnapshotter + "-batch-activity-alias"
)
