package batch

import (
	"context"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type ActivityAlias string

const (
	FetchNextLocalCSVSourceBatchAlias string = "fetch-next-" + batch.LocalCSVSource + "-batch-alias"
	FetchNextCloudCSVSourceBatchAlias string = "fetch-next-" + batch.CloudCSVSource + "-batch-alias"
	WriteNextNoopSinkBatchAlias       string = "write-next-" + batch.NoopSink + "-batch-alias"
	WriteNextMongoSinkBatchAlias      string = "write-next-" + batch.MongoSink + "-batch-alias"
)

// FetchNextActivity fetches the next batch of data from the source.
// It builds the source from the provided configuration, fetches the next batch,
// and records a heartbeat for the activity.
func FetchNextActivity[T any, S batch.SourceConfig[T]](
	ctx context.Context,
	in *batch.FetchInput[T, S],
) (*batch.FetchOutput[T], error) {
	l := activity.GetLogger(ctx)
	l.Debug("FetchNextActivity started", "input-offset", in.Offset)

	src, err := in.Source.BuildSource(ctx)
	if err != nil {
		l.Error("error building source", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	// Ensure the source is closed after use
	defer func() {
		if err := src.Close(ctx); err != nil {
			l.Error("error closing source", "error", err.Error())
		}
	}()

	// Fetch the next batch from the source
	b, err := src.Next(ctx, in.Offset, in.BatchSize)
	if err != nil {
		l.Error("error fetching next batch", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// record activity heartbeat
	activity.RecordHeartbeat(ctx, in)

	return &batch.FetchOutput[T]{
		Batch: b,
	}, nil
}

// WriteActivity writes a batch of data to the sink.
// It builds the sink from the provided configuration, writes the batch,
// and records a heartbeat for the activity.
func WriteActivity[T any, D batch.SinkConfig[T]](
	ctx context.Context,
	in *batch.WriteInput[T, D],
) (*batch.WriteOutput[T], error) {
	l := activity.GetLogger(ctx)
	l.Debug("WriteActivity started", "start-offset", in.Batch.StartOffset, "next-offset", in.Batch.NextOffset)

	sk, err := in.Sink.BuildSink(ctx)
	if err != nil {
		l.Error("error building sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	defer func() {
		if err := sk.Close(ctx); err != nil {
			l.Error("error closing sink", "error", err.Error())
		}
	}()

	// Write the batch to the sink
	out, err := sk.Write(ctx, in.Batch)
	if err != nil {
		l.Error("error writing to sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// record activity heartbeat
	activity.RecordHeartbeat(ctx, in)

	return &batch.WriteOutput[T]{
		Batch: out,
	}, nil
}
