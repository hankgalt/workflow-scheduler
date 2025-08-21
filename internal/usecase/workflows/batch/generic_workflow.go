package batch

import (
	"container/list"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
)

// A single generic orchestration.
// You get strong typing across the whole path.
func ProcessBatchWorkflow[T any, S batch.SourceConfig[T], D batch.SinkConfig[T]](
	ctx workflow.Context,
	req batch.BatchRequest[T, S, D],
) (batch.BatchRequest[T, S, D], error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	if req.Batches == nil {
		req.Batches = map[string]*batch.BatchProcess[T]{}
	}

	if req.Offsets == nil {
		req.Offsets = []uint64{}
		req.Offsets = append(req.Offsets, req.StartAt)
	}

	if req.MaxBatches < 2 {
		req.MaxBatches = 2
	}

	// initiate a new queue
	q := list.New()

	// Execute the fetch activity for first batch
	var fetched batch.FetchOutput[T]
	if err := workflow.ExecuteActivity(
		ctx, FetchNextActivity[T, S],
		batch.FetchInput[T, S]{
			Source:    req.Source,
			Offset:    req.Offsets[len(req.Offsets)-1],
			BatchSize: req.BatchSize,
		}).Get(ctx, &fetched); err != nil {
		return req, err
	}

	// Update request state
	req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
	req.Batches[fmt.Sprintf("batch-%d-%d", fetched.Batch.StartOffset, fetched.Batch.NextOffset)] = fetched.Batch
	req.Done = fetched.Batch.Done

	// Execute async the write activity for first batch
	future := workflow.ExecuteActivity(ctx, WriteActivity[T, D], batch.WriteInput[T, D]{
		Sink:  req.Sink,
		Batch: fetched.Batch,
	})

	q.PushBack(future)

	// while there are items in queue
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(req.MaxBatches) && !req.Done {
			if err := workflow.ExecuteActivity(
				ctx,
				FetchNextActivity[T, S],
				batch.FetchInput[T, S]{
					Source:    req.Source,
					Offset:    req.Offsets[len(req.Offsets)-1],
					BatchSize: req.BatchSize,
				},
			).Get(ctx, &fetched); err != nil {
				return req, err
			}

			// Update request state
			req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
			req.Done = fetched.Batch.Done

			// Execute async the write activity for first batch
			future := workflow.ExecuteActivity(ctx, WriteActivity[T, D], batch.WriteInput[T, D]{
				Sink:  req.Sink,
				Batch: fetched.Batch,
			})

			q.PushBack(future)

		} else {
			future, ok := q.Remove(q.Front()).(workflow.Future)
			if !ok {
				return req, errors.New("failed to remove future from queue")
			}

			var wOut batch.WriteOutput[T]
			if err := future.Get(ctx, &wOut); err != nil {
				return req, err
			}

			batchId := fmt.Sprintf("batch-%d-%d", wOut.Batch.StartOffset, wOut.Batch.NextOffset)
			if _, ok := req.Batches[batchId]; !ok {
				req.Batches[batchId] = wOut.Batch
			} else {
				req.Batches[batchId] = wOut.Batch
			}
		}

	}

	// for {
	// 	// 1) Fetch
	// 	var fetched batch.FetchOutput[T]
	// 	if err := workflow.ExecuteActivity(ctx, FetchNextActivity[T, S], batch.FetchInput[T, S]{
	// 		Source: req.Source, Offset: offset, BatchSize: req.BatchSize,
	// 	}).Get(ctx, &fetched); err != nil {
	// 		return err
	// 	}

	// 	// 2) Transform (optional)
	// 	var tb batch.BatchProcess[T]
	// 	if err := workflow.ExecuteActivity(ctx, TransformActivity[T], fetched.Batch).Get(ctx, &tb); err != nil {
	// 		return err
	// 	}

	// 	// 3) Write
	// 	if len(tb.Records) > 0 {
	// 		if err := workflow.ExecuteActivity(ctx, WriteActivity[T, D], batch.WriteInput[T, D]{
	// 			Sink:  req.Sink,
	// 			Batch: tb,
	// 		}).Get(ctx, nil); err != nil {
	// 			return err
	// 		}
	// 	}

	// 	if tb.Done {
	// 		return nil
	// 	}
	// 	offset = tb.NextOffset

	// 	// Continue-as-new guard to cap history
	// 	if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 8_000 {
	// 		return workflow.NewContinueAsNewError(ctx, ProcessBatchWorkflow[T, S, D], batch.BatchRequest[T, S, D]{
	// 			JobID: req.JobID, BatchSize: req.BatchSize, StartAt: offset, Source: req.Source, Sink: req.Sink,
	// 		})
	// 	}
	// }

	return req, nil
}
