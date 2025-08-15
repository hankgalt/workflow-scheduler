package sandbox

import (
	"go.temporal.io/sdk/workflow"
)

const (
	ERR_PROCESS_CONTINUE_AS_NEW_WKFL = "error running continue as new workflow"
)

type ContinueAsNewForBatchInput struct {
	Counter  uint32
	RunCount uint
}

func ContinueAsNewForBatch(
	ctx workflow.Context,
	req *ContinueAsNewForBatchInput,
) (*ContinueAsNewForBatchInput, error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ContinueAsNewForBatch workflow started", "request", req)

	req.RunCount++

	i := 0
	for i < 5 {
		req.Counter++
		i++
	}

	if req.Counter < 25 {
		return nil, workflow.NewContinueAsNewError(ctx, ContinueAsNewForBatch, req)
	}

	return req, nil
}
