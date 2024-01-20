package business

import (
	"fmt"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const DEFAULT_BATCH_BUFFER int = 2

// ReadCSVWorkflow workflow decider
func ReadCSVWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ReadCSVWorkflow - started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy),
		zap.String("run-id", req.RunId),
		zap.String("wkfl-id", req.WorkflowId),
		zap.String("p-run-id", req.ProcessRunId),
		zap.String("p-wkfl-id", req.ProcessWorkflowId))

	count := 0
	configErr := false
	resp, err := readCSV(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *workflow.GenericError:
			l.Error("ReadCSVWorkflow - cadence generic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *workflow.TimeoutError:
			l.Error("ReadCSVWorkflow - time out error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *cadence.CustomError:
			l.Error("ReadCSVWorkflow - cadence custom error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Reason() {
			case common.ERR_SESSION_CTX:
				resp, err = readCSV(ctx, resp)
				continue
			case common.ERR_WRONG_HOST:
				configErr = true
				return req, err
			case common.ERR_MISSING_FILE_NAME:
				configErr = true
				return req, err
			case common.ERR_MISSING_REQSTR:
				configErr = true
				return req, err
			case ERR_MISSING_OFFSETS:
				configErr = true
				return req, err
			default:
				resp, err = readCSV(ctx, resp)
				continue
			}
		case *workflow.PanicError:
			l.Error("ReadCSVWorkflow - cadence panic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *cadence.CanceledError:
			l.Error("ReadCSVWorkflow - cadence canceled error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("ReadCSVWorkflow - other error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			resp, err = readCSV(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ReadCSVWorkflow - failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("ReadCSVWorkflow failed", err)
	}

	l.Debug(
		"ReadCSVWorkflow - completed",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

func readCSV(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// check for same host
	if hostId != HostID {
		l.Error("ReadCSVWorkflow -  - running on wrong host",
			zap.String("file", req.FileName),
			zap.String("req-host", hostId),
			zap.String("curr-host", HostID))

		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return nil, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return nil, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	if len(req.OffSets) < 1 {
		l.Error(ERR_MISSING_OFFSETS)
		return req, cadence.NewCustomError(ERR_MISSING_OFFSETS, ErrMissingOffsets)
	}

	executionDuration := common.ONE_DAY

	batchBuffer := DEFAULT_BATCH_BUFFER
	pendingFutures := []workflow.Future{}

	scwo := workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: executionDuration,
	}
	cwCtx := workflow.WithChildOptions(ctx, scwo)

	// for each offset in CSV state, start ReadCSVRecordsWorkflow child workflow
	for i, offset := range req.OffSets {
		readReReq := models.ReadRecordsParams{
			FileName:    req.FileName,
			RequestedBy: req.RequestedBy,
			HostID:      req.HostID,
			RunId:       req.ProcessRunId,
			WorkflowId:  req.ProcessWorkflowId,
			Type:        req.Type,
			Headers:     req.Headers,
			BatchIndex:  i,
			Start:       offset,
		}
		if i >= len(req.OffSets)-1 {
			readReReq.End = req.FileSize
		} else {
			readReReq.End = req.OffSets[i+1]
		}

		future := workflow.ExecuteChildWorkflow(cwCtx, ReadCSVRecordsWorkflow, &readReReq)
		pendingFutures = append(pendingFutures, future)

		l.Debug(
			"ReadCSVWorkflow - batch processing loop status",
			zap.Int("inProcess", len(pendingFutures)),
			zap.Int("buffSize", batchBuffer),
			zap.Int("total-batches", len(req.OffSets)),
			zap.Int("curBatchIdx", i),
			zap.Int("remain-batches", len(req.OffSets)-i))

		if len(pendingFutures) >= batchBuffer {
			for j, f := range pendingFutures {
				var resp models.ReadRecordsParams
				err := f.Get(ctx, &resp)
				if err != nil {
					l.Error(
						"ReadCSVWorkflow - error processing csv records batch",
						zap.Error(err),
						zap.String("file", req.FileName),
						zap.Int64("start", req.OffSets[i-batchBuffer+j]))
					if sigErr := sendCSVBatchSignal(ctx, nil, err); sigErr != nil {
						l.Error(
							"ReadCSVWorkflow - error sending csv batch err signal",
							zap.Error(err),
							zap.String("file", req.FileName),
							zap.Int64("start", req.OffSets[i-batchBuffer+j]))
					}
				} else {
					req.Results[resp.Start] = &models.CSVBatchResult{
						BatchIndex: resp.BatchIndex,
						Start:      resp.Start,
						End:        resp.End,
						Results:    resp.Results,
						Errors:     resp.Errors,
					}
					if sigErr := sendCSVBatchSignal(ctx, &resp, nil); sigErr != nil {
						l.Error(
							"ReadCSVWorkflow - error sending csv batch done signal",
							zap.Error(err),
							zap.String("file", req.FileName),
							zap.Int64("start", req.OffSets[i-batchBuffer+j]))
					} else {
						l.Debug(
							"ReadCSVWorkflow - csv batch done signal sent",
							zap.String("file", req.FileName))
					}
				}
			}

			pendingFutures = []workflow.Future{}

			if len(req.OffSets)-i-1 < batchBuffer {
				batchBuffer = len(req.OffSets) - 1 - i
				l.Info("ReadCSVWorkflow - last batch info", zap.Any("size", batchBuffer), zap.Any("currIdx", i))
			}
		}
	}
	l.Info("ReadCSVWorkflow - done", zap.Any("result", len(req.Results)))
	return req, nil
}

func sendCSVBatchSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	err error,
) error {
	fmt.Println()
	fmt.Printf("ReadCSVWorkflow - sendCSVBatchSignal runId: %s, workflowId: %s\n", req.RunId, req.WorkflowId)
	fmt.Println()

	// result & error channel names
	resultChanName := fmt.Sprintf("%s-csv-batch-ch", req.FileName)
	errChanName := fmt.Sprintf("%s-csv-batch-err-ch", req.FileName)

	if err != nil {
		var result interface{}
		if sigErr := workflow.SignalExternalWorkflow(ctx, req.WorkflowId, req.RunId, errChanName, err).Get(ctx, result); sigErr != nil {
			return sigErr
		}
		return nil
	} else if req != nil {
		var result interface{}
		if sigErr := workflow.SignalExternalWorkflow(ctx, req.WorkflowId, req.RunId, resultChanName, req).Get(ctx, result); sigErr != nil {
			return sigErr
		}
		return nil
	}
	return ErrNoSignalToSend
}
