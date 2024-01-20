package business

import (
	"fmt"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	ERR_STORAGE_CLIENT = "error storage client"
	ERR_READING_FILE   = "error reading file"
)

var (
	ErrStorageClient = errors.NewAppError(ERR_STORAGE_CLIENT)
)

const CSV_BATCH_STATUS_SIG = "csvBatchStatusSig"

// ProcessCSVWorkflow processes an entity records csv file for type: AGENT, PRINCIPAL & FILING
// retiries for configured number of times on failures exculding configuration errors
func ProcessCSVWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"ProcessCSVWorkflow - started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy),
		zap.Any("type", req.Type))

	count := 0
	configErr := false
	resp, err := processCSV(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *workflow.GenericError:
			l.Error("ProcessCSVWorkflow - cadence generic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *workflow.TimeoutError:
			l.Error("ProcessCSVWorkflow - time out error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *cadence.CustomError:
			l.Error("ProcessCSVWorkflow - cadence custom error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Reason() {
			case common.ERR_SESSION_CTX:
				resp, err = processCSV(ctx, resp)
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
			case common.ERR_MISSING_FILE:
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
			l.Error("ProcessCSVWorkflow - cadence panic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *cadence.CanceledError:
			l.Error("ProcessCSVWorkflow - cadence canceled error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("ProcessCSVWorkflow - other error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			resp, err = processCSV(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessCSVWorkflow - failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("ReadCSVWorkflow failed", err)
	}

	l.Debug(
		"ProcessCSVWorkflow - completed",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

// processCSV is the main workflow routine for processing csv files
func processCSV(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"ProcessCSVWorkflow - workflow execution started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy),
		zap.Any("type", req.Type))

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (models.CSVInfoState, error) {
		return models.MapCSVInfoToState(req), nil
	}); err != nil {
		l.Info("ProcessCSVWorkflow - SetQueryHandler failed", zap.Error(err))
		return req, cadence.NewCustomError(common.ERR_QUERY_HANDLER, err)
	}

	// setup results map
	if req.Results == nil {
		req.Results = map[int64]*models.CSVBatchResult{}
	}

	// setup workflow & run references
	if req.ProcessRunId == "" {
		req.ProcessRunId = workflow.GetInfo(ctx).WorkflowExecution.RunID
	}
	if req.ProcessWorkflowId == "" {
		req.ProcessWorkflowId = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	l.Debug(
		"ProcessCSVWorkflow - building CSV headers",
		zap.String("file", req.FileName),
		zap.String("run-id", req.RunId),
		zap.String("wkfl-id", req.WorkflowId),
		zap.String("p-run-id", req.ProcessRunId),
		zap.String("p-wkfl-id", req.ProcessWorkflowId))
	// get csv header
	req, err := ExecuteGetCSVHeadersActivity(ctx, req)
	if err != nil {
		l.Error("ProcessCSVWorkflow - error getting headers", zap.Error(err), zap.String("file", req.FileName), zap.String("run-id", req.RunId), zap.String("wkfl-id", req.WorkflowId))
		return req, err
	}
	l.Debug("ProcessCSVWorkflow - built headers, building offsets", zap.Any("headers", req.Headers), zap.String("file", req.FileName), zap.String("run-id", req.RunId), zap.String("wkfl-id", req.WorkflowId))

	// get csv batch offsets
	req, err = ExecuteGetCSVOffsetsActivity(ctx, req)
	if err != nil {
		l.Error("ProcessCSVWorkflow - error getting offsets", zap.Error(err), zap.String("file", req.FileName))
		return req, err
	}
	l.Debug("ProcessCSVWorkflow - built offsets, setting up CSV read ", zap.Any("offsets", req.OffSets), zap.Any("file-size", req.FileSize), zap.String("file", req.FileName))

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

	// // setup csvRBP go-routine status update channels
	// csvBatchCh := workflow.NewNamedChannel(ctx, fmt.Sprintf("%s-csv-batch-ch", req.FileName))
	// csvBatchErrCh := workflow.NewNamedChannel(ctx, fmt.Sprintf("%s-csv-batch-err-ch", req.FileName))

	// // setup csvRecP go-routine status update channels
	// csvResultCh := workflow.NewNamedChannel(ctx, fmt.Sprintf("%s-csv-result-ch", req.FileName))
	// csvErrCh := workflow.NewNamedChannel(ctx, fmt.Sprintf("%s-csv-err-ch", req.FileName))

	defer func() {
		l.Debug("ProcessCSVWorkflow - closing ProcessCSVWorkflow channels", zap.String("file", req.FileName))
		readGRStatusChannel.Close()
		batchGRStatusChannel.Close()
		recordGRStatusChannel.Close()
		batchGRStatusForRChannel.Close()

		csvBatchCh.Close()
		csvBatchErrCh.Close()

		csvResultCh.Close()
		csvErrCh.Close()
		l.Debug("ProcessCSVWorkflow - done closing ProcessCSVWorkflow channels", zap.String("file", req.FileName))
	}()

	// setup component go-routines completion flags
	csvProcessed := false
	batchesProcessed := false
	recordsProcessed := false

	// setup counts
	count := 0
	recordCount := 0
	batchCount := 0

	l.Debug("ProcessCSVWorkflow - starting ReadCSVWorkflow child workflow routine", zap.String("file", req.FileName), zap.Any("offsets", req.OffSets))
	// csv read processing(csvRP) go-routine,  on completion, sends csv status update signal
	workflow.Go(ctx, func(chctx workflow.Context) {
		// build child workflow context
		scwo := workflow.ChildWorkflowOptions{
			ExecutionStartToCloseTimeout: common.ONE_DAY,
		}
		cwCtx := workflow.WithChildOptions(chctx, scwo)

		future := workflow.ExecuteChildWorkflow(cwCtx, ReadCSVWorkflow, req)
		// l.Info("ReadCSVWorkflow child workflow started")

		var resp models.CSVInfo
		err := future.Get(chctx, &resp)
		if err != nil {
			l.Error("ProcessCSVWorkflow - error executing ReadCSVWorkflow child workflow", zap.Error(err), zap.String("file", req.FileName))
		} else {
			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range resp.Results {
				errCount = errCount + v.ErrCount
				resultCount = resultCount + v.ResultCount
				recCount = recCount + v.Count
			}
			l.Debug(
				"ProcessCSVWorkflow - ReadCSVWorkflow child workflow response",
				zap.String("file", req.FileName),
				zap.Any("batches", resp.OffSets),
				zap.Any("results", resp.Results),
				zap.Int("count", count),
				zap.Int("record-count", recordCount))
			l.Debug(
				"ProcessCSVWorkflow - ReadCSVWorkflow child workflow response",
				zap.String("file", req.FileName),
				zap.Any("errCount", errCount),
				zap.Any("resultCount", resultCount),
				zap.Int("recCount", recCount))
			readGRStatusChannel.Send(chctx, resp)
			return
		}
	})

	l.Debug("ProcessCSVWorkflow - starting CSV batch update listener routine", zap.String("file", req.FileName))
	// csv record batch processing(csvRBP) go-routine,  on completion, sends csv batch status update signal
	workflow.Go(ctx, func(chctx workflow.Context) {
		errCount := 0

		// listen for csv batch reads, errors & context done
		for {
			s := workflow.NewSelector(chctx)

			// batch result update
			s.AddReceive(csvBatchCh, func(c workflow.Channel, ok bool) {
				if ok {
					var resSig models.ReadRecordsParams
					ok := c.Receive(chctx, &resSig)
					if ok {
						batchCount++
						count = count + resSig.Count
						l.Debug(
							"ProcessCSVWorkflow - batch routine - received csv batch result signal",
							zap.String("file", req.FileName),
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
						l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch result signal", zap.String("file", req.FileName))
					}
				}
			})

			// batch error update
			s.AddReceive(csvBatchErrCh, func(c workflow.Channel, ok bool) {
				if ok {
					var errSig error
					ok := c.Receive(chctx, errSig)
					if ok {
						errCount++
						l.Error("ProcessCSVWorkflow - batch routine - received csv batch error signal", zap.Error(errSig), zap.String("file", req.FileName))
					} else {
						l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch error signal", zap.String("file", req.FileName))
					}
				}
			})

			// context update
			s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
				l.Info("ProcessCSVWorkflow - batch routine - context done.", zap.String("ctx-err", chctx.Err().Error()), zap.String("file", req.FileName))
			})

			s.Select(chctx)

			// completion condition: number of completed & error batches should match total number of batches
			l.Debug(
				"ProcessCSVWorkflow - batch routine - batch status",
				zap.Int("batches", len(req.OffSets)),
				zap.Int("batches-processed", batchCount),
				zap.Int("count", count),
				zap.Int("record-count", recordCount),
				zap.Int("err-count", errCount))
			if batchCount+errCount >= len(req.OffSets) {
				l.Debug(
					"ProcessCSVWorkflow - batch routine - all batches processed.",
					zap.Int("count", count),
					zap.Bool("recordsProcessed", recordsProcessed),
					zap.Int("record-count", recordCount),
					zap.Int("err-count", errCount),
					zap.Int("batches", len(req.OffSets)),
					zap.Int("batches-processed", batchCount))
				batchGRStatusChannel.Send(chctx, req.Results)
				return
			}
		}
	})

	l.Debug("ProcessCSVWorkflow - starting CSV record update listener routine", zap.String("file", req.FileName))
	// csv record processing(csvRecP) go-routine
	workflow.Go(ctx, func(chctx workflow.Context) {
		// listen for csv record reads, errors & context done

		for {
			s := workflow.NewSelector(chctx)

			// csv record result update
			s.AddReceive(csvResultCh, func(c workflow.Channel, ok bool) {
				if ok {
					var resSig models.CSVResultSignal
					ok := c.Receive(chctx, &resSig)
					if ok {
						// update record count
						recordCount++
						l.Debug(
							"ProcessCSVWorkflow - record routine - received csv record result signal",
							zap.String("file", req.FileName),
							zap.Any("record", resSig.Record),
							zap.Int64("start", resSig.Start),
							zap.Int64("size", resSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving record result signal", zap.String("file", req.FileName))
					}
				}
			})

			// csv record error update
			s.AddReceive(csvErrCh, func(c workflow.Channel, ok bool) {
				if ok {
					var errSig models.CSVErrSignal
					ok := c.Receive(chctx, &errSig)
					if ok {
						// update record count
						recordCount++
						l.Debug(
							"ProcessCSVWorkflow - record routine - received csv record error signal",
							zap.String("error", errSig.Error),
							zap.Int64("start", errSig.Start),
							zap.Int64("size", errSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving record error signal", zap.String("file", req.FileName))
					}
				}
			})

			// context update
			s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
				l.Info("ProcessCSVWorkflow - record routine - context done.", zap.String("ctx-err", chctx.Err().Error()), zap.String("file", req.FileName))
			})

			s.AddReceive(batchGRStatusForRChannel, func(c workflow.Channel, ok bool) {
				if ok {
					var sig bool
					ok := c.Receive(chctx, &sig)
					if ok {
						l.Debug(
							"ProcessCSVWorkflow - record routine - record processing received batch status signal",
							zap.Any("signal", sig), zap.String("file", req.FileName))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving batch status signal", zap.String("file", req.FileName))
					}
				}
			})

			s.Select(chctx)

			// completion condition:
			l.Debug(
				"ProcessCSVWorkflow - record routine - records processing",
				zap.Bool("batchesProcessed", batchesProcessed),
				zap.Int("count", count),
				zap.Int("record-count", recordCount))
			if recordCount >= count && batchesProcessed {
				l.Info("ProcessCSVWorkflow - record routine - all records processed, records processing go-routine returning", zap.String("file", req.FileName))
				recordGRStatusChannel.Send(chctx, fmt.Sprintf("count: %d, recordCount: %d", count, recordCount))
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
					l.Debug(
						"ProcessCSVWorkflow - received CSV status signal",
						zap.String("file", req.FileName),
						zap.Any("results", csvStatusSig.Results),
						zap.Int("count", count),
						zap.Int("recordCnt", recordCount))
					req.Results = csvStatusSig.Results
					csvProcessed = true
				} else {
					l.Error("ProcessCSVWorkflow - error receiving CSV status signal.", zap.Error(err), zap.String("file", req.FileName))
				}
			}
		})
		s.AddReceive(batchGRStatusChannel, func(c workflow.Channel, ok bool) {
			if ok {
				var batchStatusSig map[int64]*models.CSVBatchResult
				c.Receive(ctx, &batchStatusSig)
				l.Debug(
					"ProcessCSVWorkflow - received CSV batch status signal",
					zap.String("file", req.FileName),
					zap.Int("count", count),
					zap.Int("recordCnt", recordCount))
				batchesProcessed = true
				batchGRStatusForRChannel.Send(ctx, true)
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV batch status signal.", zap.Error(err), zap.String("file", req.FileName))
			}
		})
		s.AddReceive(recordGRStatusChannel, func(c workflow.Channel, ok bool) {
			if ok {
				var recordGRSig string
				c.Receive(ctx, &recordGRSig)
				l.Debug(
					"ProcessCSVWorkflow - received CSV record status signal",
					zap.String("file", req.FileName),
					zap.Any("status", recordGRSig),
					zap.Int("count", count),
					zap.Int("recordCnt", recordCount))
				recordsProcessed = true
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV record status signal.", zap.Error(err), zap.String("file", req.FileName))
			}
		})
		s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
			l.Info("ProcessCSVWorkflow - context done.", zap.String("ctx-err", ctx.Err().Error()), zap.String("file", req.FileName))
		})

		s.Select(ctx)

		l.Info("ProcessCSVWorkflow - status", zap.Bool("csvProcessed", csvProcessed), zap.Bool("batchesProcessed", batchesProcessed), zap.Bool("recordsProcessed", recordsProcessed))
		if csvProcessed && batchesProcessed && recordsProcessed {
			l.Info("ProcessCSVWorkflow - csv, batches & records processed, main loop completed, returning results", zap.Int("batches-processed", len(req.Results)), zap.String("file", req.FileName))
			break
		}
	}

	return req, nil
}

func batchUpdateListener(ctx workflow.Context, req *models.CSVInfo, batchGRStatusChannel workflow.Channel) {
	l := workflow.GetLogger(ctx)
	// setup csvRBP go-routine status update channels
	csvBatchCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-batch-ch", req.FileName))
	csvBatchErrCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("%s-csv-batch-err-ch", req.FileName))
	errCount := 0

	// listen for csv batch reads, errors & context done
	for {
		s := workflow.NewSelector(ctx)

		// batch result update
		s.AddReceive(csvBatchCh, func(c workflow.Channel, ok bool) {
			if ok {
				var resSig models.ReadRecordsParams
				ok := c.Receive(ctx, &resSig)
				if ok {
					l.Debug(
						"ProcessCSVWorkflow - batch routine - received csv batch result signal",
						zap.String("file", req.FileName),
						zap.Any("batchIdx", resSig.BatchIndex),
						zap.Int64("start", resSig.Start),
						zap.Int64("end", resSig.End),
						zap.Int("batches", len(req.OffSets)),
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
					l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch result signal", zap.String("file", req.FileName))
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
					l.Error("ProcessCSVWorkflow - batch routine - received csv batch error signal", zap.Error(errSig), zap.String("file", req.FileName))
				} else {
					l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch error signal", zap.String("file", req.FileName))
				}
			}
		})

		// context update
		s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
			l.Info("ProcessCSVWorkflow - batch routine - context done.", zap.String("ctx-err", ctx.Err().Error()), zap.String("file", req.FileName))
		})

		s.Select(ctx)

		// completion condition: number of completed & error batches should match total number of batches
		// l.Debug(
		// 	"ProcessCSVWorkflow - batch routine - batch status",
		// 	zap.Int("batches", len(req.OffSets)),
		// 	zap.Int("err-count", errCount))
		// if batchCount+errCount >= len(req.OffSets) {
		// 	l.Debug(
		// 		"ProcessCSVWorkflow - batch routine - all batches processed.",
		// 		zap.Int("err-count", errCount),
		// 		zap.Int("batches", len(req.OffSets)))
		// 	batchGRStatusChannel.Send(ctx, req.Results)
		// 	return
		// }
	}
}
