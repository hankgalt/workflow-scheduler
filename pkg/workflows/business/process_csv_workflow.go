package business

import (
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/errors"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const (
	ERR_STORAGE_CLIENT   string = "error storage client"
	ERR_PROCESS_CSV_WKFL string = "error process csv workflow"
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
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy),
		slog.Any("type", req.Type))

	count := 0
	resp, err := processCSV(ctx, req)
	for err != nil && count < 10 {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error("ProcessCSVWorkflow - temporal generic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.TimeoutError:
			l.Error("ProcessCSVWorkflow - time out error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.ApplicationError:
			l.Error("ProcessCSVWorkflow - temporal application error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Type() {
			case comwkfl.ERR_SESSION_CTX:
				resp, err = processCSV(ctx, resp)
				continue
			case comwkfl.ERR_WRONG_HOST:
				return req, err
			case comwkfl.ERR_MISSING_FILE_NAME:
				return req, err
			case comwkfl.ERR_MISSING_REQSTR:
				return req, err
			case comwkfl.ERR_MISSING_FILE:
				return req, err
			case ERR_MISSING_START_OFFSET:
				return req, err
			case ERR_BUILDING_OFFSETS:
				return req, err
			case ERR_MISSING_OFFSETS:
				return req, err
			case comwkfl.ERR_MISSING_SCHEDULER_CLIENT:
				return req, err
			default:
				resp, err = processCSV(ctx, resp)
				continue
			}
		case *temporal.PanicError:
			l.Error("ProcessCSVWorkflow - temporal panic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		case *temporal.CanceledError:
			l.Error("ProcessCSVWorkflow - temporal canceled error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		default:
			l.Error("ProcessCSVWorkflow - other error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			resp, err = processCSV(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessCSVWorkflow - failed",
			slog.String("err-msg", err.Error()),
			slog.Int("tries", count),
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_PROCESS_CSV_WKFL, ERR_PROCESS_CSV_WKFL, errors.WrapError(err, ERR_PROCESS_CSV_WKFL))
	}

	l.Debug(
		"ProcessCSVWorkflow - completed",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy))
	return resp, nil
}

// processCSV processes entity records csv file for type: AGENT, PRINCIPAL & FILING
// builds headers & offsets and updates workflow state,
// sets up channels for batch & record processing signals,
// starts separate go-routines for csv read child workflow, batch signal listener & record signal listener,
// starts main go-routine to coordinateP workflow status update
// updates state with results & returns updated state
func processCSV(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"ProcessCSVWorkflow - workflow execution started",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy),
		slog.Any("type", req.Type))

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (models.CSVInfoState, error) {
		return models.MapCSVInfoToState(req), nil
	}); err != nil {
		l.Info("ProcessCSVWorkflow - SetQueryHandler failed", slog.Any("error", err))
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_QUERY_HANDLER, comwkfl.ERR_QUERY_HANDLER, errors.WrapError(err, comwkfl.ERR_QUERY_HANDLER))
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
		slog.String("file", req.FileName),
		slog.String("run-id", req.RunId),
		slog.String("wkfl-id", req.WorkflowId),
		slog.String("p-run-id", req.ProcessRunId),
		slog.String("p-wkfl-id", req.ProcessWorkflowId))
	// get csv header
	req, err := ExecuteGetCSVHeadersActivity(ctx, req)
	if err != nil {
		l.Error("ProcessCSVWorkflow - error getting headers", slog.Any("error", err), slog.String("file", req.FileName), slog.String("run-id", req.RunId), slog.String("wkfl-id", req.WorkflowId))
		return req, err
	}
	l.Debug("ProcessCSVWorkflow - built headers, building offsets", slog.Any("headers", req.Headers), slog.String("file", req.FileName), slog.String("run-id", req.RunId), slog.String("wkfl-id", req.WorkflowId))

	// get csv batch offsets
	req, err = ExecuteGetCSVOffsetsActivity(ctx, req)
	if err != nil {
		l.Error("ProcessCSVWorkflow - error getting offsets", slog.Any("error", err), slog.String("file", req.FileName))
		return req, err
	}
	l.Debug("ProcessCSVWorkflow - built offsets, setting up CSV read ", slog.Any("offsets", req.OffSets), slog.Any("file-size", req.FileSize), slog.String("file", req.FileName))

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
		l.Debug("ProcessCSVWorkflow - closing ProcessCSVWorkflow channels", slog.String("file", req.FileName))
		readGRStatusChannel.Close()
		batchGRStatusChannel.Close()
		recordGRStatusChannel.Close()
		batchGRStatusForRChannel.Close()

		l.Debug("ProcessCSVWorkflow - done closing ProcessCSVWorkflow channels", slog.String("file", req.FileName))
	}()

	// setup component go-routines completion flags
	csvProcessed := false
	batchesProcessed := false
	recordsProcessed := false

	// setup counts
	count := 0
	recordCount := 0
	batchCount := 0

	l.Debug("ProcessCSVWorkflow - starting ReadCSVWorkflow child workflow routine", slog.String("file", req.FileName), slog.Any("offsets", req.OffSets))
	// csv read processing(csvRP) go-routine,  on completion, sends csv status update signal
	workflow.Go(ctx, func(chctx workflow.Context) {
		// build child workflow context
		scwo := workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: comwkfl.ONE_DAY,
		}
		cwCtx := workflow.WithChildOptions(chctx, scwo)

		future := workflow.ExecuteChildWorkflow(cwCtx, ReadCSVWorkflow, req)
		// l.Info("ReadCSVWorkflow child workflow started")

		var resp models.CSVInfo
		err := future.Get(chctx, &resp)
		if err != nil {
			l.Error("ProcessCSVWorkflow - error executing ReadCSVWorkflow child workflow", slog.Any("error", err), slog.String("file", req.FileName))
		} else {
			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range resp.Results {
				errCount = errCount + len(v.Errors)
				resultCount = resultCount + len(v.Results)
				recCount = recCount + len(v.Errors) + len(v.Results)
			}
			l.Debug(
				"ProcessCSVWorkflow - ReadCSVWorkflow child workflow response",
				slog.String("file", req.FileName),
				slog.Any("batches", resp.OffSets),
				slog.Any("results", resp.Results),
				slog.Int("count", count),
				slog.Int("record-count", recordCount))
			l.Debug(
				"ProcessCSVWorkflow - ReadCSVWorkflow child workflow response",
				slog.String("file", req.FileName),
				slog.Any("errCount", errCount),
				slog.Any("resultCount", resultCount),
				slog.Int("recCount", recCount))
			readGRStatusChannel.Send(chctx, resp)
			return
		}
	})

	l.Debug("ProcessCSVWorkflow - starting CSV batch update listener routine", slog.String("file", req.FileName))
	// csv record batch processing(csvRBP) go-routine,  on completion, sends csv batch status update signal
	workflow.Go(ctx, func(chctx workflow.Context) {
		errCount := 0

		// listen for csv batch reads, errors & context done
		for {
			s := workflow.NewSelector(chctx)

			// batch result update
			s.AddReceive(csvBatchCh, func(c workflow.ReceiveChannel, ok bool) {
				if ok {
					var resSig models.ReadRecordsParams
					ok := c.Receive(chctx, &resSig)
					if ok {
						batchCount++
						resSigCount := len(resSig.Errors) + len(resSig.Results)
						count = count + resSigCount
						l.Debug(
							"ProcessCSVWorkflow - batch routine - received csv batch result signal",
							slog.String("file", req.FileName),
							slog.Any("batchIdx", resSig.BatchIndex),
							slog.Int64("start", resSig.Start),
							slog.Int64("end", resSig.End),
							slog.Int("batches", len(req.OffSets)),
							slog.Int("batches-processed", batchCount),
							slog.Int("count", resSigCount),
							slog.Int("result", len(resSig.Results)),
							slog.Int("err", len(resSig.Errors)))
						req.Results[resSig.Start] = &models.CSVBatchResult{
							BatchIndex: resSig.BatchIndex,
							Start:      resSig.Start,
							End:        resSig.End,
							Results:    resSig.Results,
							Errors:     resSig.Errors,
						}
					} else {
						l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch result signal", slog.String("file", req.FileName))
					}
				}
			})

			// batch error update
			s.AddReceive(csvBatchErrCh, func(c workflow.ReceiveChannel, ok bool) {
				if ok {
					var errSig error
					ok := c.Receive(chctx, errSig)
					if ok {
						errCount++
						l.Error("ProcessCSVWorkflow - batch routine - received csv batch error signal", slog.Any("error", errSig), slog.String("file", req.FileName))
					} else {
						l.Error("ProcessCSVWorkflow - batch routine - error receiving csv batch error signal", slog.String("file", req.FileName))
					}
				}
			})

			// context update
			s.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, ok bool) {
				l.Info("ProcessCSVWorkflow - batch routine - context done.", slog.String("ctx-err", chctx.Err().Error()), slog.String("file", req.FileName))
			})

			s.Select(chctx)

			// completion condition: number of completed & error batches should match total number of batches
			l.Debug(
				"ProcessCSVWorkflow - batch routine - batch status",
				slog.Int("batches", len(req.OffSets)),
				slog.Int("batches-processed", batchCount),
				slog.Int("count", count),
				slog.Int("record-count", recordCount),
				slog.Int("err-count", errCount))
			if batchCount+errCount >= len(req.OffSets) {
				l.Debug(
					"ProcessCSVWorkflow - batch routine - all batches processed.",
					slog.Int("count", count),
					slog.Bool("recordsProcessed", recordsProcessed),
					slog.Int("record-count", recordCount),
					slog.Int("err-count", errCount),
					slog.Int("batches", len(req.OffSets)),
					slog.Int("batches-processed", batchCount))
				batchGRStatusChannel.Send(chctx, req.Results)
				return
			}
		}
	})

	l.Debug("ProcessCSVWorkflow - starting CSV record update listener routine", slog.String("file", req.FileName))
	// csv record processing(csvRecP) go-routine
	workflow.Go(ctx, func(chctx workflow.Context) {
		// listen for csv record reads, errors & context done

		for {
			s := workflow.NewSelector(chctx)

			// csv record result update
			s.AddReceive(csvResultCh, func(c workflow.ReceiveChannel, ok bool) {
				if ok {
					var resSig models.CSVResultSignal
					ok := c.Receive(chctx, &resSig)
					if ok {
						// update record count
						recordCount++
						l.Debug(
							"ProcessCSVWorkflow - record routine - received csv record result signal",
							slog.String("file", req.FileName),
							slog.Any("resultId", resSig.Id),
							slog.Int64("start", resSig.Start),
							slog.Int64("size", resSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving record result signal", slog.String("file", req.FileName))
					}
				}
			})

			// csv record error update
			s.AddReceive(csvErrCh, func(c workflow.ReceiveChannel, ok bool) {
				if ok {
					var errSig models.CSVErrSignal
					ok := c.Receive(chctx, &errSig)
					if ok {
						// update record count
						recordCount++
						l.Debug(
							"ProcessCSVWorkflow - record routine - received csv record error signal",
							slog.String("error", errSig.Error),
							slog.Int64("start", errSig.Start),
							slog.Int64("size", errSig.Size))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving record error signal", slog.String("file", req.FileName))
					}
				}
			})

			// context update
			s.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, ok bool) {
				l.Info("ProcessCSVWorkflow - record routine - context done.", slog.String("ctx-err", chctx.Err().Error()), slog.String("file", req.FileName))
			})

			s.AddReceive(batchGRStatusForRChannel, func(c workflow.ReceiveChannel, ok bool) {
				if ok {
					var sig bool
					ok := c.Receive(chctx, &sig)
					if ok {
						l.Debug(
							"ProcessCSVWorkflow - record routine - record processing received batch status signal",
							slog.Any("signal", sig), slog.String("file", req.FileName))
					} else {
						l.Error("ProcessCSVWorkflow - record routine - error receiving batch status signal", slog.String("file", req.FileName))
					}
				}
			})

			s.Select(chctx)

			// completion condition:
			l.Debug(
				"ProcessCSVWorkflow - record routine - records processing",
				slog.Bool("batchesProcessed", batchesProcessed),
				slog.Int("count", count),
				slog.Int("record-count", recordCount))
			if recordCount >= count && batchesProcessed {
				l.Info("ProcessCSVWorkflow - record routine - all records processed, records processing go-routine returning", slog.String("file", req.FileName))
				recordGRStatusChannel.Send(chctx, fmt.Sprintf("count: %d, recordCount: %d", count, recordCount))
				return
			}
		}
	})

	// main go routine, listens for csvRP, csvRBP & csRecP go-routine's status update
	for {
		s := workflow.NewSelector(ctx)
		s.AddReceive(readGRStatusChannel, func(c workflow.ReceiveChannel, ok bool) {
			if ok {
				var csvStatusSig models.CSVInfo
				ok := c.Receive(ctx, &csvStatusSig)
				if ok {
					l.Debug(
						"ProcessCSVWorkflow - received CSV status signal",
						slog.String("file", req.FileName),
						slog.Any("results", csvStatusSig.Results),
						slog.Int("count", count),
						slog.Int("recordCnt", recordCount))
					req.Results = csvStatusSig.Results
					csvProcessed = true
				} else {
					l.Error("ProcessCSVWorkflow - error receiving CSV status signal.", slog.Any("error", err), slog.String("file", req.FileName))
				}
			}
		})
		s.AddReceive(batchGRStatusChannel, func(c workflow.ReceiveChannel, ok bool) {
			if ok {
				var batchStatusSig map[int64]*models.CSVBatchResult
				c.Receive(ctx, &batchStatusSig)
				l.Debug(
					"ProcessCSVWorkflow - received CSV batch status signal",
					slog.String("file", req.FileName),
					slog.Int("count", count),
					slog.Int("recordCnt", recordCount))
				batchesProcessed = true
				batchGRStatusForRChannel.Send(ctx, true)
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV batch status signal.", slog.Any("error", err), slog.String("file", req.FileName))
			}
		})
		s.AddReceive(recordGRStatusChannel, func(c workflow.ReceiveChannel, ok bool) {
			if ok {
				var recordGRSig string
				c.Receive(ctx, &recordGRSig)
				l.Debug(
					"ProcessCSVWorkflow - received CSV record status signal",
					slog.String("file", req.FileName),
					slog.Any("status", recordGRSig),
					slog.Int("count", count),
					slog.Int("recordCnt", recordCount))
				recordsProcessed = true
			} else {
				l.Error("ProcessCSVWorkflow - error receiving CSV record status signal.", slog.Any("error", err), slog.String("file", req.FileName))
			}
		})
		s.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, ok bool) {
			l.Info("ProcessCSVWorkflow - context done.", slog.String("ctx-err", ctx.Err().Error()), slog.String("file", req.FileName))
		})

		s.Select(ctx)

		l.Info("ProcessCSVWorkflow - status", slog.Bool("csvProcessed", csvProcessed), slog.Bool("batchesProcessed", batchesProcessed), slog.Bool("recordsProcessed", recordsProcessed))
		if csvProcessed && batchesProcessed && recordsProcessed {
			l.Info("ProcessCSVWorkflow - csv, batches & records processed, main loop completed, returning results", slog.Int("batches-processed", len(req.Results)), slog.String("file", req.FileName))
			break
		}
	}

	return req, nil
}
