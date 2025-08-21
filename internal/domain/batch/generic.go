package batch

import (
	"context"
)

// Domain "record" type that moves through the pipeline.
type CSVRow map[string]string

type BatchResult struct {
	Result any
	Error  string
}

type BatchRecord[T any] struct {
	Data        T
	Start, End  uint64
	BatchResult BatchResult
}

// BatchProcess[T any] is the neutral "batch process" unit.
type BatchProcess[T any] struct {
	BatchId     string
	Records     []*BatchRecord[T]
	StartOffset uint64 // start offset in the source
	NextOffset  uint64 // cursor / next-page token / byte offset
	Error       string
	Done        bool
}

// SourceConfig[T any] is a config that *knows how to build* a Source for a specific T.
type SourceConfig[T any] interface {
	BuildSource(ctx context.Context) (Source[T], error)
	Name() string
}

// Source[T any] is a source of batches of T, e.g., a CSV file, a database table, etc.
// Pulls the next batch given an offset; return next offset.
type Source[T any] interface {
	Next(ctx context.Context, offset uint64, n uint) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}

// SinkConfig[T any] is a config that *knows how to build* a Sink for a specific T.
type SinkConfig[T any] interface {
	BuildSink(ctx context.Context) (Sink[T], error)
	Name() string
}

// Sink[T any] is a sink that writes a batch of T to a destination, e.g., a database, a file, etc.
// Writes a batch and return side info (e.g., count written, last id).
type Sink[T any] interface {
	Write(ctx context.Context, b *BatchProcess[T]) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}

// WriteInput[T any, D SinkConfig[T]] is the input for the WriteActivity.
type WriteInput[T any, D SinkConfig[T]] struct {
	Sink  D
	Batch *BatchProcess[T]
}

// WriteOutput is the output for the WriteActivity.
type WriteOutput[T any] struct {
	Batch *BatchProcess[T]
}

// FetchInput[T any, S SourceConfig[T]] is the input for the FetchNextActivity.
type FetchInput[T any, S SourceConfig[T]] struct {
	Source    S
	Offset    uint64
	BatchSize uint
}

// FetchOutput[T any] is the output for the FetchNextActivity.
type FetchOutput[T any] struct {
	Batch *BatchProcess[T]
}

// BatchRequest[T any, S SourceConfig[T], D SinkConfig[T]] is a request to process a batch of T from a source S and write to a sink D.
type BatchRequest[T any, S SourceConfig[T], D SinkConfig[T]] struct {
	MaxBatches uint                        // maximum number of batches to process
	BatchSize  uint                        // maximum size of each batch
	JobID      string                      // unique identifier for the job
	StartAt    uint64                      // initial offset
	Source     S                           // source configuration
	Sink       D                           // sink configuration
	Done       bool                        // whether the job is done
	Offsets    []uint64                    // list of offsets for each batch
	Batches    map[string]*BatchProcess[T] // map of batch by ID
}

// A request to process a batch of CSVRow from a local CSV file.
type LocalCSVRequest = BatchRequest[CSVRow, LocalCSVConfig, NoopSinkConfig[CSVRow]]

// A request to process a batch of CSVRow from a cloud CSV source (S3/GCS/Azure).
type CloudCSVRequest = BatchRequest[CSVRow, CloudCSVConfig, NoopSinkConfig[CSVRow]]

// A request to process a batch of CSVRow and write to a MongoDB sink.
type LocalCSVMongoRequest = BatchRequest[CSVRow, LocalCSVConfig, MongoSinkConfig[CSVRow]]

// A request to process a batch of CSVRow from a cloud CSV source and write to a MongoDB sink.
type CloudCSVMongoRequest = BatchRequest[CSVRow, CloudCSVConfig, MongoSinkConfig[CSVRow]]
