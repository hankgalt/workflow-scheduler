package batch

import (
	"container/list"
	"fmt"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ERR_PROCESS_CLOUD_CSV_MONGO_WKFL = "error running process cloud CSV to mongo workflow"
)

func ProcessCloudCSVMongo(
	ctx workflow.Context,
	req *batch.CloudCSVMongoBatchRequest,
) (*batch.CloudCSVMongoBatchRequest, error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ProcessCloudCSVMongo workflow started")

	count := 0
	resp, err := processCloudCSVMongo(ctx, req)
	for err != nil && count < 5 {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error(
				"ProcessCloudCSVMongo - temporal server error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			return req, err
		case *temporal.TimeoutError:
			l.Error(
				"ProcessCloudCSVMongo - temporal time out error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			return req, err
		case *temporal.PanicError:
			l.Error(
				"ProcessCloudCSVMongo - temporal panic error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			return resp, err
		case *temporal.CanceledError:
			l.Error(
				"ProcessCloudCSVMongo - temporal canceled error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			return resp, err
		case *temporal.ApplicationError:
			l.Error(
				"ProcessCloudCSVMongo - temporal application error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			switch wkflErr.Type() {
			case ERROR_INVALID_CONFIG_TYPE:
				return req, err
			// TODO add adiitional error cases
			default:
				resp, err = processCloudCSVMongo(ctx, resp)
				continue
			}
		default:
			l.Error(
				"ProcessCloudCSVMongo - other error",
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
			resp, err = processCloudCSVMongo(ctx, resp)
			continue
		}
	}

	if err != nil {
		l.Error(
			"ProcessCloudCSVMongo - failed",
			"error", err.Error(),
			"tries", count,
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_PROCESS_CLOUD_CSV_MONGO_WKFL, ERR_PROCESS_CLOUD_CSV_MONGO_WKFL, err)
	}

	return resp, nil

}

func processCloudCSVMongo(
	ctx workflow.Context,
	req *batch.CloudCSVMongoBatchRequest,
) (*batch.CloudCSVMongoBatchRequest, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"processCloudCSVMongo - started processing cloud CSV to mongo batch request",
		"request", req,
	)

	if req.MaxBatches < 2 {
		req.MaxBatches = 2
	}

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	if req.End <= req.Start {
		// setup first batch in current request
		reqCfg, err := ExecuteSetupCloudCSVMongoBatchActivity(ctx, req.Config, req.RequestConfig)
		if err != nil {
			l.Error(
				"processCloudCSVMongo - error setting up batch",
				"error", err.Error(),
			)
			return req, err
		}
		l.Debug(
			"processCloudCSVMongo - first batch setup",
			"offsets", reqCfg.Offsets,
			"headers", reqCfg.Headers,
		)
		req.RequestConfig = reqCfg

		// build batch request
		start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
		batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
		if err != nil {
			l.Error(
				"processCloudCSVMongo - error generating first batch ID",
				"error", err.Error())
			return req, err
		}
		if _, ok := req.Batches[batchID]; !ok {
			batch := &batch.Batch{
				BatchID: batchID,
				Start:   start,
				End:     end,
			}

			// update request batch state
			req.Batches[batchID] = batch
		}
		l.Debug(
			"processCloudCSVMongo - first batch request",
			"batchID", batchID,
			"start", start,
			"end", end,
		)

		// start async execution of process batch activity & push future to queue
		future := AsyncExecuteHandleCloudCSVMongoBatchDataActivity(
			ctx,
			req.Config,
			req.RequestConfig,
			req.Batches[batchID],
		)
		q.PushBack(future)

		// while there are items in queue
		for q.Len() > 0 {
			// if we have less than max batches and the request is not yet completed
			if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
				// setup next batch
				l.Debug(
					"processCloudCSVMongo - batch setup before",
					"offsets", req.Offsets,
				)
				reqCfg, err := ExecuteSetupCloudCSVMongoBatchActivity(ctx, req.Config, req.RequestConfig)
				if err != nil {
					l.Error(
						"processCloudCSVMongo - error setting up next batch",
						"error", err.Error(),
					)
					return req, err
				}
				l.Debug(
					"processCloudCSVMongo - batch setup after",
					"offsets", reqCfg.Offsets,
				)
				req.RequestConfig = reqCfg

				// build batch request
				start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
				batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
				if err != nil {
					l.Error(
						"processCloudCSVMongo - error generating batch ID",
						"error", err.Error(),
					)
					return req, err
				}
				if _, ok := req.Batches[batchID]; !ok {
					batch := &batch.Batch{
						BatchID: batchID,
						Start:   start,
						End:     end,
					}

					// update request batch state
					req.Batches[batchID] = batch
				}
				l.Debug(
					"processCloudCSVMongo - next batch request",
					"batchID", batchID,
					"start", start,
					"end", end,
				)

				// start async execution of process batch activity & push future to queue
				future := AsyncExecuteHandleCloudCSVMongoBatchDataActivity(
					ctx,
					req.Config,
					req.RequestConfig,
					req.Batches[batchID],
				)
				q.PushBack(future)

				l.Debug(
					"processCloudCSVMongo - queue length",
					"items", q.Len(),
					"offsets", req.Offsets,
				)
			} else {
				// if we have more than max batches or the request is completed,
				// pull the next future from the queue
				future := q.Remove(q.Front()).(workflow.Future)
				var batchResult *batch.Batch
				err := future.Get(ctx, &batchResult)
				if err != nil {
					l.Error(
						"processCloudCSVMongo - error processing batch data",
						"error", err.Error(),
					)
				} else {
					// update the request with the batch result
					l.Debug(
						"processCloudCSVMongo - pulled batch result",
						"items", batchResult,
						"offsets", req.Offsets,
					)
					req.Batches[batchResult.BatchID] = batchResult
				}
			}
		}
	} else {
		// is workflow retry
		l.Info(
			"processCloudCSVMongo - retry - request state",
			"offsets", req.Offsets,
			"start", req.Start,
			"end", req.End,
		)

		// initialize counter
		i := 0

		// build first batch Id
		start, end := req.Offsets[i], req.Offsets[i+1]
		batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
		if err != nil {
			l.Error(
				"processCloudCSVMongo - error generating batch ID",
				"error", err.Error(),
			)
			return req, err
		}

		// start async execution of process batch activity & push future to queue
		future := AsyncExecuteHandleCloudCSVMongoBatchDataActivity(
			ctx,
			req.Config,
			req.RequestConfig,
			req.Batches[batchID],
		)
		q.PushBack(future)

		// increment counter
		i++

		// while there are items in queue
		for q.Len() > 0 {
			if q.Len() < int(req.MaxBatches) && i < len(req.Batches) {
				// build next batch request
				start, end := req.Offsets[i], req.Offsets[i+1]
				batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
				if err != nil {
					l.Error(
						"processCloudCSVMongo - error generating batch ID",
						"error", err.Error(),
					)
					return req, err
				}

				// start async execution of process batch activity & push future to queue
				future := AsyncExecuteHandleCloudCSVMongoBatchDataActivity(
					ctx,
					req.Config,
					req.RequestConfig,
					req.Batches[batchID],
				)
				q.PushBack(future)

				// increment counter
				i++
			} else {
				// pull the next future from the queue
				future := q.Remove(q.Front()).(workflow.Future)
				var batchResult *batch.Batch
				err := future.Get(ctx, &batchResult)
				if err != nil {
					l.Error(
						"processCloudCSVMongo - error processing batch data",
						"error", err.Error(),
					)
				} else {
					// update the request with the batch result
					l.Debug(
						"processCloudCSVMongo - pulled batch result",
						"items", batchResult,
						"offsets", req.Offsets,
					)
					req.Batches[batchResult.BatchID] = batchResult
				}
			}
		}
	}

	return req, nil
}
