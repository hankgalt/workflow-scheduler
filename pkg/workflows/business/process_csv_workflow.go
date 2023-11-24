package business

import (
	"fmt"
	"time"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	ERR_MISSING_FILE   = "error missing file"
	ERR_STORAGE_CLIENT = "error storage client"
	ERR_READING_FILE   = "error reading file"
)

var (
	ErrStorageClient = errors.NewAppError(ERR_STORAGE_CLIENT)
)

const CSV_BATCH_STATUS_SIG = "csvBatchStatusSig"

// ProcessCSVWorkflow workflow decider
func ProcessCSVWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ProcessCSVWorkflow started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))

	count := 0
	configErr := false
	resp, err := processCSV(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *workflow.GenericError:
			l.Error("cadence generic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *workflow.TimeoutError:
			l.Error("time out error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *cadence.CustomError:
			l.Error("cadence custom error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Reason() {
			case common.ERR_SESSION_CTX:
				resp, err = processCSV(ctx, resp)
				continue
			case ERR_WRONG_HOST:
				configErr = true
				return req, err
			case ERR_MISSING_FILE_NAME:
				configErr = true
				return req, err
			case ERR_MISSING_REQSTR:
				configErr = true
				return req, err
			case ERR_MISSING_FILE:
				configErr = true
				return req, err
			case ERR_MISSING_START_OFFSET:
				configErr = true
				return req, err
			case ERR_BUILDING_OFFSETS:
				configErr = true
				return req, err
			case ERR_MISSING_OFFSETS:
				configErr = true
				return req, err
			case common.ERR_MISSING_SCHEDULER_CLIENT:
				configErr = true
				return req, err
			default:
				resp, err = processCSV(ctx, resp)
				continue
			}
		case *workflow.PanicError:
			l.Error("cadence panic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *cadence.CanceledError:
			l.Error("cadence canceled error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("other error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			resp, err = processCSV(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessCSVWorkflow failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("ReadCSVWorkflow failed", err)
	}

	l.Info(
		"ProcessCSVWorkflow completed",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

func processCSV(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)

	// setup results map
	if req.Results == nil {
		req.Results = map[int64]*models.CSVBatchResult{}
	}

	// set execution duration
	executionDuration := common.ONE_DAY

	// build session context
	so := &workflow.SessionOptions{
		CreationTimeout:  10 * time.Minute,
		ExecutionTimeout: executionDuration,
	}
	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		l.Error(common.ERR_SESSION_CTX, zap.Error(err))
		return req, cadence.NewCustomError(common.ERR_SESSION_CTX, err)
	}
	sessionCtx = workflow.WithStartToCloseTimeout(sessionCtx, executionDuration)
	defer workflow.CompleteSession(sessionCtx)

	// setup workflow & run references
	req.RunId = workflow.GetInfo(ctx).WorkflowExecution.RunID
	req.WorkflowId = workflow.GetInfo(ctx).WorkflowExecution.ID

	// get csv header
	req, err = ExecuteGetCSVHeadersActivity(sessionCtx, req)
	if err != nil {
		l.Error("ReadCSVWorkflow error getting headers", zap.String("error", err.Error()))
		return req, err
	}
	l.Info("ReadCSVWorkflow headers", zap.Any("headers", req.Headers))

	// get csv batch offsets
	req, err = ExecuteGetCSVOffsetsActivity(sessionCtx, req)
	if err != nil {
		l.Error("ReadCSVWorkflow error getting offsets", zap.String("error", err.Error()))
		return req, err
	}
	l.Info("ReadCSVWorkflow offsets", zap.Int("offsets", len(req.OffSets)), zap.Any("file-size", req.FileSize))

	// setup workflow component channels
	readGRStatusChannel := workflow.NewChannel(ctx)
	batchGRStatusChannel := workflow.NewChannel(ctx)
	recordGRStatusChannel := workflow.NewChannel(ctx)
	batchGRStatusForRChannel := workflow.NewChannel(ctx)

	// setup csvRBP go-routine status update channels
	csvBatchCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-batch-ch", req.FileName))
	csvBatchErrCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-batch-err-ch", req.FileName))

	// setup csvRecP go-routine status update channels
	csvResultCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-result-ch", req.FileName))
	csvErrCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-err-ch", req.FileName))
	defer func() {
		l.Info("closing ReadCSVWorkflow channels")
		readGRStatusChannel.Close()
		batchGRStatusChannel.Close()
		recordGRStatusChannel.Close()
		batchGRStatusForRChannel.Close()

		csvBatchCh.Close()
		csvBatchErrCh.Close()

		csvResultCh.Close()
		csvErrCh.Close()
	}()

	// setup component go-routines completion flags
	csvProcessed := false
	batchesProcessed := false
	recordsProcessed := false

	// setup counts
	count := 0
	recordCount := 0
	batchCount := 0

	// csv read processing(csvRP) go-routine,  on completion, sends csv status update signal
	workflow.Go(sessionCtx, func(chctx workflow.Context) {
		// build child workflow context
		scwo := workflow.ChildWorkflowOptions{
			ExecutionStartToCloseTimeout: executionDuration,
		}
		cwCtx := workflow.WithChildOptions(chctx, scwo)

		future := workflow.ExecuteChildWorkflow(cwCtx, ReadCSVWorkflow, req)
		// l.Info("ReadCSVWorkflow child workflow started")

		var resp models.CSVInfo
		err := future.Get(chctx, &resp)
		if err != nil {
			l.Error("error executing ReadCSVWorkflow child workflow")
		} else {
			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range req.Results {
				errCount = errCount + v.ErrCount
				resultCount = resultCount + v.ResultCount
				recCount = recCount + v.Count
			}
			l.Info(
				"ReadCSVWorkflow response",
				zap.Int64("size", req.FileSize),
				zap.Int("batches", len(resp.OffSets)),
				zap.Int("batches-processed", len(resp.Results)),
				zap.Int("count", count),
				zap.Int("record-count", recordCount),
				zap.Int("errCount", errCount),
				zap.Int("resultCount", resultCount),
				zap.Int("recCount", recCount))
			readGRStatusChannel.Send(chctx, resp)
			return
		}
	})

	// csv record batch processing(csvRBP) go-routine,  on completion, sends csv batch status update signal
	workflow.Go(sessionCtx, func(chctx workflow.Context) {
		errCount := 0

		// listen for csv batch reads, errors & context done
		for {
			s := workflow.NewSelector(chctx)

			// batch result update
			s.AddReceive(csvBatchCh, func(c workflow.Channel, ok bool) {
				if ok {
					var resSig models.ReadRecordsParams
					ok := c.Receive(ctx, &resSig)
					if ok {
						batchCount++
						count = count + resSig.Count
						l.Info(
							"ProcessCSVWorkflow - received csv batch result signal",
							zap.Any("batchIdx", resSig.BatchIndex),
							zap.Int64("start", resSig.Start),
							zap.Int64("end", resSig.End),
							zap.Int("batches", len(req.OffSets)),
							zap.Int("batches-processed", batchCount),
							zap.Int("count", resSig.Count),
							zap.Int("result", resSig.ResultCount),
							zap.Int("err", resSig.ErrCount))
						req.Results[resSig.Start] = &models.CSVBatchResult{
							BatchIndex:  resSig.BatchIndex,
							Start:       resSig.Start,
							End:         resSig.End,
							Count:       resSig.Count,
							ResultCount: resSig.ResultCount,
							ErrCount:    resSig.ErrCount,
						}
					} else {
						l.Error("ProcessCSVWorkflow - error receiving csv batch result signal")
					}
				}
			})

			// batch error update
			s.AddReceive(csvBatchErrCh, func(c workflow.Channel, ok bool) {
				if ok {
					var errSig error
					ok := c.Receive(ctx, errSig)
					if ok {
						errCount++
						l.Error("ProcessCSVWorkflow - received csv batch error signal", zap.Error(errSig))
					} else {
						l.Error("ProcessCSVWorkflow - error receiving csv batch error signal")
					}
				}
			})

			// context update
			s.AddReceive(chctx.Done(), func(c workflow.Channel, ok bool) {
				l.Info("ProcessCSVWorkflow - context done.", zap.String("ctx-err", chctx.Err().Error()))
			})

			s.Select(chctx)

			// completion condition: number of completed & error batches should match total number of batches
			l.Info(
				"ProcessCSVWorkflow - batch status",
				zap.Int("batches", len(req.OffSets)),
				zap.Int("batches-processed", batchCount),
				zap.Int("count", count),
				zap.Int("record-count", recordCount),
				zap.Int("err-count", errCount))
			if batchCount+errCount >= len(req.OffSets) {
				l.Info(
					"ProcessCSVWorkflow - all batches processed.",
					zap.Int("count", count),
					zap.Bool("recordsProcessed", recordsProcessed),
					zap.Int("record-count", recordCount),
					zap.Int("err-count", errCount),
					zap.Int("batches", len(req.OffSets)),
					zap.Int("batches-processed", batchCount))
				batchGRStatusChannel.Send(ctx, req.Results)
				return
			}
		}
	})

	// csv record processing(csvRecP) go-routine
	workflow.Go(sessionCtx, func(chctx workflow.Context) {
		// listen for csv record reads, errors & context done

		for {
			s := workflow.NewSelector(chctx)

			// csv record result update
			s.AddReceive(csvResultCh, func(c workflow.Channel, ok bool) {
				if ok {
					var resSig models.CSVResultSignal
					ok := c.Receive(ctx, &resSig)
					if ok {
						// update record count
						recordCount++
						// l.Info(
						// 	"ProcessCSVWorkflow - received csv record result signal",
						// 	zap.Any("record", resSig.Record),
						// 	zap.Int64("start", resSig.Start),
						// 	zap.Int64("size", resSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - error receiving record result signal")
					}
				}
			})

			// csv record error update
			s.AddReceive(csvErrCh, func(c workflow.Channel, ok bool) {
				if ok {
					var errSig models.CSVErrSignal
					ok := c.Receive(ctx, &errSig)
					if ok {
						// update record count
						recordCount++
						l.Info(
							"ProcessCSVWorkflow - received csv record error signal",
							zap.String("error", errSig.Error),
							zap.Int64("start", errSig.Start),
							zap.Int64("size", errSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - error receiving record error signal")
					}
				}
			})

			s.AddReceive(batchGRStatusForRChannel, func(c workflow.Channel, ok bool) {
				if ok {
					var sig bool
					ok := c.Receive(ctx, &sig)
					if ok {
						l.Info(
							"ProcessCSVWorkflow - record processing received batch status signal",
							zap.Any("signal", sig))
					} else {
						l.Error("ProcessCSVWorkflow - record processing, error receiving batch status signal")
					}
				}
			})

			// context update
			s.AddReceive(chctx.Done(), func(c workflow.Channel, ok bool) {
				l.Info("ProcessCSVWorkflow - context done.", zap.String("ctx-err", chctx.Err().Error()))
			})

			s.Select(chctx)

			// completion condition:
			// l.Info(
			// 	"ProcessCSVWorkflow - records processing",
			// 	zap.Bool("batchesProcessed", batchesProcessed),
			// 	zap.Int("count", count),
			// 	zap.Int("record-count", recordCount))
			if recordCount >= count && batchesProcessed {
				l.Info("ProcessCSVWorkflow - all records processed, records processing go-routine returning")
				recordGRStatusChannel.Send(ctx, fmt.Sprintf("count: %d, recordCount: %d", count, recordCount))
				return
			}
		}
	})

	// main go routine, listens for csvRP, csvRBP & csRecP go-routine's status update
	for {
		s := workflow.NewSelector(ctx)
		s.AddReceive(readGRStatusChannel, func(c workflow.Channel, ok bool) {
			if ok {
				var csvStatusSig models.CSVInfo
				ok := c.Receive(ctx, &csvStatusSig)
				if ok {
					l.Info(
						"ProcessCSVWorkflow - received CSV status signal",
						zap.Int("count", count),
						zap.Int("recordCnt", recordCount))
					// req.Results = csvStatusSig.Results
					csvProcessed = true
				} else {
					l.Error("ProcessCSVWorkflow - error receiving CSV status signal.")
				}
			}
		})
		s.AddReceive(batchGRStatusChannel, func(c workflow.Channel, ok bool) {
			if ok {
				var batchStatusSig map[int64]*models.CSVBatchResult
				c.Receive(ctx, &batchStatusSig)
				l.Info(
					"ProcessCSVWorkflow - received CSV batch status signal",
					zap.Int("count", count),
					zap.Int("recordCnt", recordCount))
				batchesProcessed = true
				batchGRStatusForRChannel.Send(ctx, true)
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV batch status signal.")
			}
		})
		s.AddReceive(recordGRStatusChannel, func(c workflow.Channel, ok bool) {
			if ok {
				var recordGRSig string
				c.Receive(ctx, &recordGRSig)
				l.Info(
					"ProcessCSVWorkflow - received CSV record status signal",
					zap.Any("status", recordGRSig),
					zap.Int("count", count),
					zap.Int("recordCnt", recordCount))
				recordsProcessed = true
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV record status signal.")
			}
		})
		s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
			l.Info("ProcessCSVWorkflow - context done.", zap.String("ctx-err", ctx.Err().Error()))
		})

		s.Select(ctx)

		if csvProcessed && batchesProcessed && recordsProcessed {
			l.Info("ProcessCSVWorkflow - csv, batches & records processed, main loop completed, returning results", zap.Int("batches-processed", len(req.Results)))
			return req, nil
		}
	}
}
