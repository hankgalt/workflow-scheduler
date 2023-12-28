package business

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const (
	ERR_NO_SIGNAL_TO_SEND = "error no signal to send"
)

var (
	ErrNoSignalToSend = errors.NewAppError(ERR_NO_SIGNAL_TO_SEND)
)

// ReadCSVRecordsWorkflow workflow decider
func ReadCSVRecordsWorkflow(ctx workflow.Context, req *models.ReadRecordsParams) (*models.ReadRecordsParams, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ReadCSVRecordsWorkflow started",
		zap.String("file", req.FileName),
		zap.Int("index", req.BatchIndex),
		zap.Int64("start", req.Start),
		zap.Int64("end", req.End),
		zap.String("reqstr", req.RequestedBy))

	count := 0
	configErr := false
	resp, err := readCSVRecords(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *workflow.GenericError:
			l.Error("ReadCSVRecordsWorkflow cadence generic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *workflow.TimeoutError:
			l.Error("ReadCSVRecordsWorkflow time out error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *cadence.CustomError:
			l.Error("ReadCSVRecordsWorkflow cadence custom error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Reason() {
			case common.ERR_SESSION_CTX:
				resp, err = readCSVRecords(ctx, resp)
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
			case ERR_UNKNOW_ENTITY_TYPE:
				configErr = true
				return req, err
			default:
				resp, err = readCSVRecords(ctx, resp)
				continue
			}
		case *workflow.PanicError:
			l.Error("ReadCSVRecordsWorkflow cadence panic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *cadence.CanceledError:
			l.Error("ReadCSVRecordsWorkflow cadence canceled error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("ReadCSVRecordsWorkflow other error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			resp, err = readCSVRecords(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ReadCSVRecordsWorkflow failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("ReadCSVWorkflow failed", err)
	}

	l.Info(
		"ReadCSVRecordsWorkflow completed",
		zap.String("file", req.FileName),
		zap.Int("index", req.BatchIndex),
		zap.Int64("start", req.Start),
		zap.Int64("end", req.End),
		zap.Int("count", req.Count),
		zap.Int("resultCount", req.ResultCount),
		zap.Int("errCount", req.ErrCount),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

func readCSVRecords(ctx workflow.Context, req *models.ReadRecordsParams) (*models.ReadRecordsParams, error) {
	l := workflow.GetLogger(ctx)

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// check for same host
	if hostId != HostID {
		logWrongHostError(req, hostId, HostID, l)
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

	// build local file path
	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	// get file
	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(common.ERR_MISSING_FILE)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE, err)
	}
	defer func() {
		err := file.Close()
		if err != nil && err != os.ErrClosed {
			l.Error("ReadCSVRecordsWorkflow error closing csv file", zap.Error(err))
		}
	}()

	// read batch content
	data := make([]byte, req.End-req.Start)
	_, err = file.ReadAt(data, req.Start)
	if err != nil {
		l.Error(ERR_READING_FILE)
		return req, cadence.NewCustomError(ERR_READING_FILE, err)
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
			// csv record error, increment read count
			req.Count++

			// calculate read data size
			size := csvReader.InputOffset() - offset

			// get record string
			var singleRec []string
			if recStr, err := ReadRecord(localFilePath, req.Start+offset, size, l); err != nil {
				l.Error("ReadCSVRecordsWorkflow error reading error record", zap.Error(err))
			} else {
				// sanitize record string
				recStr = strings.Replace(recStr, "\"", "", -1)

				// read single csv record from record string
				singleRec, err = readSingleRecord(recStr)
				if err != nil {
					l.Error("ReadCSVRecordsWorkflow error reading single record", zap.Error(err))
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

					if recStr, err := ReadRecord(localFilePath, req.Start+offset, size, l); err != nil {
						l.Error("error reading record string", zap.Error(err))
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

				addEntityAndSignal(ctx, req, req.Start+offset, size, singleRec, l)
			} else {
				// send error signal
				if sigErr := sendCSVRecordSignal(ctx, req, req.Start+offset, size, nil, err); sigErr != nil {
					logCSVErrSigError(sigErr, req, req.Start+offset, size, l)
				} else {
					logCSVErrSig(req, req.Start+offset, size, l)
					req.ErrCount++
				}
			}
		} else {
			// csv record read, increment read count
			req.Count++

			// calculate read data size
			size := csvReader.InputOffset() - offset

			if len(req.Headers.Headers) != len(record) {
				// TODO fix last three columns for business filings
				dataPath, ok := ctx.Value(DataPathContextKey).(string)
				if !ok || dataPath == "" {
					dataPath = "data"
				}
				localFilePath := filepath.Join(dataPath, req.FileName)

				if recStr, err := ReadRecord(localFilePath, req.Start+offset, size, l); err != nil {
					l.Error("error reading record string", zap.Error(err))
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

			addEntityAndSignal(ctx, req, req.Start+offset, size, record, l)
		}

		offset = csvReader.InputOffset()
	}
}

func addEntityAndSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	start, end int64,
	record []string,
	l logger.AppLogger,
) {
	if fields, err := mapToInterface(req.Headers.Headers, record); err == nil {
		var activityErr error
		if req.Type == api.EntityType_AGENT {
			_, activityErr = ExecuteAddAgentActivity(ctx, fields)
		} else if req.Type == api.EntityType_PRINCIPAL {
			_, activityErr = ExecuteAddPrincipalActivity(ctx, fields)
		} else if req.Type == api.EntityType_FILING {
			_, activityErr = ExecuteAddFilingActivity(ctx, fields)
		} else {
			activityErr = cadence.NewCustomError(ERR_UNKNOW_ENTITY_TYPE, ErrUnknownEntity)
		}

		if activityErr != nil {
			logCSVResultEntityError(activityErr, req, start, end, l)

			// send error signal
			if sigErr := sendCSVRecordSignal(ctx, req, start, end, nil, activityErr); sigErr != nil {
				logCSVErrSigError(sigErr, req, start, end, l)
			} else {
				logCSVErrSig(req, start, end, l)
				req.ErrCount++
			}
		} else {
			// send result signal
			if sigErr := sendCSVRecordSignal(ctx, req, start, end, record, nil); sigErr != nil {
				logCSVResultSigError(sigErr, req, start, end, l)
			} else {
				// logCSVResultSig(req, start, end, l)
				req.ResultCount++
			}
		}
	} else {
		logCSVResultFieldsError(err, req, start, end, l)

		// send error signal
		if sigErr := sendCSVRecordSignal(ctx, req, start, end, nil, err); sigErr != nil {
			logCSVErrSigError(sigErr, req, start, end, l)
		} else {
			logCSVErrSig(req, start, end, l)
			req.ErrCount++
		}
	}
}

func sendCSVRecordSignal(
	ctx workflow.Context,
	req *models.ReadRecordsParams,
	start, size int64,
	record []string,
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
	} else if len(record) > 0 {
		var result interface{}
		if sigErr := workflow.SignalExternalWorkflow(ctx, req.WorkflowId, req.RunId, resultChanName, &models.CSVResultSignal{
			Record: record,
			Start:  start,
			Size:   size,
		}).Get(ctx, result); sigErr != nil {
			return sigErr
		}
		return nil
	}
	return ErrNoSignalToSend
}

func logCSVResultSigError(err error, req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Error(
		"ReadCSVRecordsWorkflow error sending csv result signal",
		zap.Error(err),
		zap.String("file", req.FileName),
		zap.Any("type", req.Type),
		zap.String("workflow", req.WorkflowId),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logCSVResultFieldsError(err error, req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Error(
		"ReadCSVRecordsWorkflow error mapping entity fields",
		zap.Error(err),
		zap.String("file", req.FileName),
		zap.Any("type", req.Type),
		zap.String("workflow", req.WorkflowId),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logCSVResultEntityError(err error, req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Error(
		"ReadCSVRecordsWorkflow error adding entity",
		zap.Error(err),
		zap.String("file", req.FileName),
		zap.Any("type", req.Type),
		zap.String("workflow", req.WorkflowId),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logCSVResultSig(req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Info(
		"ReadCSVRecordsWorkflow csv result signal sent",
		zap.String("file", req.FileName),
		zap.String("workflow", req.WorkflowId),
		zap.Any("type", req.Type),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logCSVErrSigError(err error, req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Error(
		"ReadCSVRecordsWorkflow error sending csv error signal",
		zap.Error(err),
		zap.String("file", req.FileName),
		zap.String("workflow", req.WorkflowId),
		zap.Any("type", req.Type),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logCSVErrSig(req *models.ReadRecordsParams, start, size int64, l logger.AppLogger) {
	l.Info(
		"ReadCSVRecordsWorkflow csv error signal sent",
		zap.String("file", req.FileName),
		zap.String("workflow", req.WorkflowId),
		zap.Int64("start", start),
		zap.Int64("size", size))
}

func logWrongHostError(req *models.ReadRecordsParams, hostId, currentHostId string, l logger.AppLogger) {
	l.Error("ReadCSVRecordsWorkflow - running on wrong host",
		zap.String("file", req.FileName),
		zap.String("req-host", hostId),
		zap.String("curr-host", currentHostId))
}
