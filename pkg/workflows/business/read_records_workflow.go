package business

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/errors"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const (
	ERR_NO_SIGNAL_TO_SEND  string = "error no signal to send"
	ERR_READING_FILE       string = "error reading file"
	ERR_CSV_RECORDS_WKFL   string = "error csv records workflow"
	ERR_UNKNOW_ENTITY_TYPE string = "error unknown business entity type"
)

var (
	ErrNoSignalToSend = errors.NewAppError(ERR_NO_SIGNAL_TO_SEND)
	ErrUnknownEntity  = errors.NewAppError(ERR_UNKNOW_ENTITY_TYPE)
)

var (
	ErrorUnknownEntity = temporal.NewApplicationErrorWithCause(ERR_UNKNOW_ENTITY_TYPE, ERR_UNKNOW_ENTITY_TYPE, ErrUnknownEntity)
)

// ReadCSVRecordsWorkflow workflow manages reading a CSV file between given offsets
// retries on configured errors, returns non-retryable errors & returns updated request state
func ReadCSVRecordsWorkflow(ctx workflow.Context, req *models.ReadRecordsParams) (*models.ReadRecordsParams, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ReadCSVRecordsWorkflow - started",
		slog.String("file", req.FileName),
		slog.Int("index", req.BatchIndex),
		slog.Int64("start", req.Start),
		slog.Int64("end", req.End),
		slog.String("reqstr", req.RequestedBy))

	count := 0
	resp, err := readCSVRecords(ctx, req)
	for err != nil && count < 10 {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error("ReadCSVRecordsWorkflow - temporal server error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.TimeoutError:
			l.Error("ReadCSVRecordsWorkflow - time out error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.ApplicationError:
			l.Error("ReadCSVRecordsWorkflow - temporal application error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Type() {
			case comwkfl.ERR_MISSING_SCHEDULER_CLIENT:
				return req, err
			case comwkfl.ERR_WRONG_HOST:
				return req, err
			case comwkfl.ERR_MISSING_FILE_NAME:
				return req, err
			case comwkfl.ERR_MISSING_REQSTR:
				return req, err
			case comwkfl.ERR_MISSING_FILE:
				return req, err
			case ERR_UNKNOW_ENTITY_TYPE:
				return req, err
			default:
				resp, err = readCSVRecords(ctx, resp)
				continue
			}
		case *temporal.PanicError:
			l.Error("ReadCSVRecordsWorkflow - temporal panic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		case *temporal.CanceledError:
			l.Error("ReadCSVRecordsWorkflow - temporal canceled error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		default:
			l.Error("ReadCSVRecordsWorkflow - other error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			resp, err = readCSVRecords(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ReadCSVRecordsWorkflow - failed",
			slog.String("err-msg", err.Error()),
			slog.Int("tries", count),
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_CSV_RECORDS_WKFL, ERR_CSV_RECORDS_WKFL, errors.WrapError(err, ERR_CSV_RECORDS_WKFL))
	}

	l.Info(
		"ReadCSVRecordsWorkflow - completed",
		slog.String("file", req.FileName),
		slog.Int("index", req.BatchIndex),
		slog.Int64("start", req.Start),
		slog.Int64("end", req.End),
		slog.Any("results", req.Results),
		slog.Any("errors", req.Errors),
		slog.String("reqstr", req.RequestedBy))
	return resp, nil
}

// readCSVRecords reads csv records from file at given start & end offsets
// sends result/error signal for each record & returns
// returns error if missing file name, requester, mismatched host & error reading file
func readCSVRecords(ctx workflow.Context, req *models.ReadRecordsParams) (*models.ReadRecordsParams, error) {
	l := workflow.GetLogger(ctx)

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// check for same host
	if hostId != HostID {
		logWrongHostError(req, hostId, HostID)
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

	// build local file path
	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	// get file
	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(comwkfl.ERR_MISSING_FILE)
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_MISSING_FILE, comwkfl.ERR_MISSING_FILE, errors.WrapError(err, comwkfl.ERR_MISSING_FILE))
	}
	defer func() {
		err := file.Close()
		if err != nil && err != os.ErrClosed {
			l.Error("ReadCSVRecordsWorkflow - error closing csv file", slog.Any("error", err))
		}
	}()

	// read batch content
	data := make([]byte, req.End-req.Start)
	_, err = file.ReadAt(data, req.Start)
	if err != nil {
		l.Error(ERR_READING_FILE)
		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, errors.WrapError(err, ERR_READING_FILE))
	}

	// create batch csv reader
	bReader := bytes.NewReader(data)
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	// get initial offset
	offset := csvReader.InputOffset()

	// read each csv record
	for i := 0; ; i = i + 1 {
		// read next csv record
		record, err := csvReader.Read()

		if err == io.EOF {
			// end of file, return
			return req, nil
		} else if err != nil {
			// calculate read data size
			size := csvReader.InputOffset() - offset

			// get record string
			var singleRec []string
			if recStr, err := ReadRecord(localFilePath, req.Start+offset, size); err != nil {
				l.Error("ReadCSVRecordsWorkflow - error reading error record", slog.Any("error", err))
			} else {
				// sanitize record string
				recStr = strings.Replace(recStr, "\"", "", -1)

				// read single csv record from record string
				singleRec, err = readSingleRecord(recStr)
				if err != nil {
					l.Error("ReadCSVRecordsWorkflow - error reading single record", slog.Any("error", err))
				}
			}

			if len(singleRec) > 0 {
				// sanitized string is a valid csv record, send result signal

				if len(req.Headers.Headers) != len(singleRec) {
					// TODO fix last three columns for business filings

					dataPath, ok := ctx.Value(DataPathContextKey).(string)
					if !ok || dataPath == "" {
						dataPath = "data"
					}
					localFilePath := filepath.Join(dataPath, req.FileName)

					if recStr, err := ReadRecord(localFilePath, req.Start+offset, size); err != nil {
						l.Error("ReadCSVRecordsWorkflow - error reading record string", slog.Any("error", err))
					} else {

						recStr = strings.ReplaceAll(recStr, "\"", "")
						recStr = strings.ReplaceAll(recStr, "\r", "")
						recStr = strings.ReplaceAll(recStr, "\n", "")

						lenVals := len(strings.Split(recStr, string(csvReader.Comma)))
						lenHeaders := len(req.Headers.Headers)

						if lenVals < lenHeaders {
							recStr = recStr + "|"
							singleRec = strings.Split(recStr, string(csvReader.Comma))
						}
					}
				}

				addEntityAndSignal(ctx, req, req.Start+offset, size, singleRec)
			} else {
				// send error signal
				if sigErr := sendCSVRecordSignal(ctx, req, req.Start+offset, size, "", err); sigErr != nil {
					logCSVErrSigError(sigErr, req, req.Start+offset, size)
				} else {
					logCSVErrSig(req, req.Start+offset, size)
					req.Errors = append(req.Errors, &models.ErrorResult{
						Start: req.Start + offset,
						End:   req.Start + offset + size,
						Error: err.Error(),
					})
				}
			}
		} else {
			// calculate read data size
			size := csvReader.InputOffset() - offset

			if len(req.Headers.Headers) != len(record) {
				// TODO fix last three columns for business filings
				dataPath, ok := ctx.Value(DataPathContextKey).(string)
				if !ok || dataPath == "" {
					dataPath = "data"
				}
				localFilePath := filepath.Join(dataPath, req.FileName)

				if recStr, err := ReadRecord(localFilePath, req.Start+offset, size); err != nil {
					l.Error("ReadCSVRecordsWorkflow - error reading record string", slog.Any("error", err))
				} else {

					recStr = strings.ReplaceAll(recStr, "\r", "")
					recStr = strings.ReplaceAll(recStr, "\n", "")

					lenVals := len(strings.Split(recStr, string(csvReader.Comma)))
					lenHeaders := len(req.Headers.Headers)

					if lenVals < lenHeaders {
						recStr = recStr + "|"
						record = strings.Split(recStr, string(csvReader.Comma))
					}
				}
			}

			addEntityAndSignal(ctx, req, req.Start+offset, size, record)
		}

		offset = csvReader.InputOffset()
	}
}

func addEntityAndSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	start, size int64,
	record []string,
) {
	if fields, err := mapToInterface(req.Headers.Headers, record); err == nil {
		var activityErr error
		var resultId string

		switch req.Type {
		case models.AGENT:
			resultId, activityErr = ExecuteAddAgentActivity(ctx, fields)
		case models.PRINCIPAL:
			resultId, activityErr = ExecuteAddPrincipalActivity(ctx, fields)
		case models.FILING:
			resultId, activityErr = ExecuteAddFilingActivity(ctx, fields)
		default:
			activityErr = ErrorUnknownEntity
		}

		if activityErr != nil {
			logCSVResultEntityError(activityErr, req, start, size)

			// send error signal
			if sigErr := sendCSVRecordSignal(ctx, req, start, size, "", activityErr); sigErr != nil {
				logCSVErrSigError(sigErr, req, start, size)
			} else {
				logCSVErrSig(req, start, size)
				req.Errors = append(req.Errors, &models.ErrorResult{
					Start: start,
					End:   start + size,
					Error: activityErr.Error(),
				})
			}
		} else {
			// send result signal
			if sigErr := sendCSVRecordSignal(ctx, req, start, size, resultId, nil); sigErr != nil {
				logCSVResultSigError(sigErr, req, start, size)
			} else {
				logCSVResultSig(req, start, size)
				req.Results = append(req.Results, &models.SuccessResult{
					Start: start,
					End:   start + size,
					Id:    resultId,
				})
			}
		}
	} else {
		logCSVResultFieldsError(err, req, start, size)

		// send error signal
		if sigErr := sendCSVRecordSignal(ctx, req, start, size, "", err); sigErr != nil {
			logCSVErrSigError(sigErr, req, start, size)
		} else {
			logCSVErrSig(req, start, size)
			req.Errors = append(req.Errors, &models.ErrorResult{
				Start: start,
				End:   start + size,
				Error: err.Error(),
			})
		}
	}
}

func sendCSVRecordSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	start, size int64,
	resultId string,
	err error,
) error {
	// result & error channel names
	resultChanName := fmt.Sprintf("%s-csv-result-ch", req.FileName)
	errChanName := fmt.Sprintf("%s-csv-err-ch", req.FileName)

	if err != nil {
		var result interface{}
		if sigErr := workflow.SignalExternalWorkflow(ctx, req.WorkflowId, req.RunId, errChanName, &models.CSVErrSignal{
			Error: err.Error(),
			Start: start,
			Size:  size,
		}).Get(ctx, result); sigErr != nil {
			return sigErr
		}
		return nil
	}

	var result interface{}
	if sigErr := workflow.SignalExternalWorkflow(ctx, req.WorkflowId, req.RunId, resultChanName, &models.CSVResultSignal{
		Id:    resultId,
		Start: start,
		Size:  size,
	}).Get(ctx, result); sigErr != nil {
		return sigErr
	}
	return nil
}

func logCSVResultSigError(err error, req *models.ReadRecordsParams, start, size int64) {
	slog.Error(
		"ReadCSVRecordsWorkflow error sending csv result signal",
		slog.Any("error", err),
		slog.String("file", req.FileName),
		slog.Any("type", req.Type),
		slog.String("workflow", req.WorkflowId),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logCSVResultFieldsError(err error, req *models.ReadRecordsParams, start, size int64) {
	slog.Error(
		"ReadCSVRecordsWorkflow error mapping entity fields",
		slog.Any("error", err),
		slog.String("file", req.FileName),
		slog.Any("type", req.Type),
		slog.String("workflow", req.WorkflowId),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logCSVResultEntityError(err error, req *models.ReadRecordsParams, start, size int64) {
	slog.Error(
		"ReadCSVRecordsWorkflow error adding entity",
		slog.Any("error", err),
		slog.String("file", req.FileName),
		slog.Any("type", req.Type),
		slog.String("workflow", req.WorkflowId),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logCSVResultSig(req *models.ReadRecordsParams, start, size int64) {
	slog.Info(
		"ReadCSVRecordsWorkflow csv result signal sent",
		slog.String("file", req.FileName),
		slog.String("workflow", req.WorkflowId),
		slog.Any("type", req.Type),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logCSVErrSigError(err error, req *models.ReadRecordsParams, start, size int64) {
	slog.Error(
		"ReadCSVRecordsWorkflow error sending csv error signal",
		slog.Any("error", err),
		slog.String("file", req.FileName),
		slog.String("workflow", req.WorkflowId),
		slog.Any("type", req.Type),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logCSVErrSig(req *models.ReadRecordsParams, start, size int64) {
	slog.Info(
		"ReadCSVRecordsWorkflow csv error signal sent",
		slog.String("file", req.FileName),
		slog.String("workflow", req.WorkflowId),
		slog.Int64("start", start),
		slog.Int64("size", size))
}

func logWrongHostError(req *models.ReadRecordsParams, hostId, currentHostId string) {
	slog.Error("ReadCSVRecordsWorkflow - running on wrong host",
		slog.String("file", req.FileName),
		slog.String("req-host", hostId),
		slog.String("curr-host", currentHostId))
}
