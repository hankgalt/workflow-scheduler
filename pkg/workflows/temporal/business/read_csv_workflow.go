package business

import (
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common"
)

const MIN_BATCH_BUFFER int = 2

const (
	ERR_READ_CSV_WKFL string = "error read csv workflow"
)

// ReadCSVWorkflow workflow manages reading a CSV file, retries on configured errors,
// returns non-retryable errors & returns updated request state
func ReadCSVWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ReadCSVWorkflow - started",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy),
		slog.String("run-id", req.RunId),
		slog.String("wkfl-id", req.WorkflowId),
		slog.String("p-run-id", req.ProcessRunId),
		slog.String("p-wkfl-id", req.ProcessWorkflowId))

	count := 0
	configErr := false
	resp, err := readCSV(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error("ReadCSVWorkflow - temporal generic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *temporal.TimeoutError:
			l.Error("ReadCSVWorkflow - time out error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *temporal.ApplicationError:
			l.Error("ReadCSVWorkflow - temporal application error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Type() {
			case comwkfl.ERR_WRONG_HOST:
				configErr = true
				return req, err
			case comwkfl.ERR_MISSING_FILE_NAME:
				configErr = true
				return req, err
			case comwkfl.ERR_MISSING_REQSTR:
				configErr = true
				return req, err
			case ERR_MISSING_OFFSETS:
				configErr = true
				return req, err
			default:
				resp, err = readCSV(ctx, resp)
				continue
			}
		case *temporal.PanicError:
			l.Error("ReadCSVWorkflow - temporal panic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *temporal.CanceledError:
			l.Error("ReadCSVWorkflow - temporal canceled error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("ReadCSVWorkflow - other error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			resp, err = readCSV(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ReadCSVWorkflow - failed",
			slog.String("err-msg", err.Error()),
			slog.Int("tries", count),
			slog.Bool("config-err", configErr),
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_READ_CSV_WKFL, ERR_READ_CSV_WKFL, errors.WrapError(err, ERR_READ_CSV_WKFL))
	}

	l.Debug(
		"ReadCSVWorkflow - completed",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy))
	return resp, nil
}

// readCSV starts concurrent ReadCSVRecordsWorkflow child workflows,
// to process records in the CSV file,
// at each given offset & partition size in the request/workflow state
// sends bactch done signal to parent workflow
// returns error if request is missing file path/requester/offset list
// populates results in the request state and returns updated request
func readCSV(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// check for same host
	if hostId != HostID {
		l.Error("ReadCSVWorkflow -  - running on wrong host",
			slog.String("file", req.FileName),
			slog.String("req-host", hostId),
			slog.String("curr-host", HostID))

		return req, comwkfl.ErrorWrongHost
	}

	if req.FileName == "" {
		l.Error(comwkfl.ERR_MISSING_FILE_NAME)
		return nil, comwkfl.ErrorMissingFileName
	}

	if req.RequestedBy == "" {
		l.Error(comwkfl.ERR_MISSING_REQSTR)
		return nil, comwkfl.ErrorMissingReqstr
	}

	if len(req.OffSets) < 1 {
		l.Error(ERR_MISSING_OFFSETS)
		return req, ErrorMissingOffsets
	}

	executionDuration := comwkfl.ONE_DAY

	batchBuffer := MIN_BATCH_BUFFER
	if req.NumBatches > MIN_BATCH_BUFFER {
		batchBuffer = req.NumBatches
	}

	if len(req.OffSets) < batchBuffer {
		batchBuffer = len(req.OffSets)
	}

	pendingFutures := []workflow.Future{}

	scwo := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: executionDuration,
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
			slog.Int("inProcess", len(pendingFutures)),
			slog.Int("buffSize", batchBuffer),
			slog.Int("total-batches", len(req.OffSets)),
			slog.Int("curBatchIdx", i),
			slog.Int("remain-batches", len(req.OffSets)-i))

		if len(pendingFutures) >= batchBuffer {
			for j, f := range pendingFutures {
				var resp models.ReadRecordsParams
				err := f.Get(ctx, &resp)
				if err != nil {
					l.Error(
						"ReadCSVWorkflow - error processing csv records batch",
						slog.Any("error", err),
						slog.String("file", req.FileName),
						slog.Int64("start", req.OffSets[i-batchBuffer+j]))
					if sigErr := sendCSVBatchSignal(ctx, nil, err); sigErr != nil {
						l.Error(
							"ReadCSVWorkflow - error sending csv batch err signal",
							slog.Any("error", err),
							slog.String("file", req.FileName),
							slog.Int64("start", req.OffSets[i-batchBuffer+j]))
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
							slog.Any("error", err),
							slog.String("file", req.FileName),
							slog.Int64("start", req.OffSets[i-batchBuffer+j]))
					} else {
						l.Debug(
							"ReadCSVWorkflow - csv batch done signal sent",
							slog.String("file", req.FileName))
					}
				}
			}

			pendingFutures = []workflow.Future{}

			if len(req.OffSets)-i-1 < batchBuffer {
				batchBuffer = len(req.OffSets) - 1 - i
				l.Info("ReadCSVWorkflow - last batch info", slog.Any("size", batchBuffer), slog.Any("currIdx", i))
			}
		}
	}
	l.Info("ReadCSVWorkflow - done", slog.Any("result", len(req.Results)))
	return req, nil
}

// sendCSVBatchSignal sends batch event signal to parent workflow
func sendCSVBatchSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	err error,
) error {
	// result & error channel names
	resultChanName := fmt.Sprintf("%s-csv-batch-ch", req.FileName)
	errChanName := fmt.Sprintf("%s-csv-batch-err-ch", req.FileName)

	slog.Debug("ReadCSVWorkflow - sendCSVBatchSignal()",
		slog.String("result-ch", resultChanName),
		slog.String("wkfl-id", req.WorkflowId),
		slog.String("run-id", req.RunId))

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
