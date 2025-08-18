package sandbox

import (
	"go.temporal.io/sdk/workflow"
)

const (
	ERR_PROCESS_CONTINUE_AS_NEW_WKFL = "error running continue as new workflow"
)

const ContinueAsNewForBatchWorkflow = "ContinueAsNewForBatch"

type ContinueAsNewForBatchInput struct {
	Counter  uint64
	RunCount uint32
}

func ContinueAsNewForBatch(
	ctx workflow.Context,
	req *ContinueAsNewForBatchInput,
	wkflEventLimit uint32,
	totalCount uint64,
) (*ContinueAsNewForBatchInput, error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ContinueAsNewForBatch workflow started", "request", req)

	// Increment continue as new run count for the workflow
	req.RunCount++

	// Initialize workflow event counter
	i := uint32(0)

	// Proceed with processing until the workflow event limit is reached
	for i < wkflEventLimit && req.Counter < totalCount {
		req.Counter++
		i++
	}

	// When workflow event limit is reached, continue as new, till a specified condition is met.
	// For example, if the counter is less than the total count.
	if req.Counter < totalCount {
		return nil, workflow.NewContinueAsNewError(ctx, ContinueAsNewForBatch, req, wkflEventLimit, totalCount)
	}

	// Once the specified condition is met, return the final result
	return req, nil
}
