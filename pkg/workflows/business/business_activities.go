package business

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/zap"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

/**
 * activities used by file processing workflow.
 */
const (
	GetCSVHeadersActivityName = "GetCSVHeadersActivity"
	GetCSVOffsetsActivityName = "GetCSVOffsetsActivity"
	AddAgentActivityName      = "AddAgentActivity"

	ReadCSVActivityName = "ReadCSVActivity"

	AddPrincipalActivityName = "AddPrincipalActivity"
	AddFilingActivityName    = "AddFilingActivity"
)

const (
	ERR_CSV_READ             = "error reading csv record"
	ERR_MISSING_START_OFFSET = "error missing start offset"
	ERR_BUILDING_OFFSETS     = "error building offsets"
	ERR_MISSING_OFFSETS      = "error missing offsets"
	ERR_MISSING_VALUE        = "error missing value"
	ERR_ADDING_AGENT         = "error adding business agent"
	ERR_ADDING_PRINCIPAL     = "error adding business principal"
	ERR_ADDING_FILING        = "error adding business filing"
	ERR_UNKNOW_ENTITY_TYPE   = "error unknown business entity type"
)

var (
	ErrMissingStartOffset = errors.NewAppError(ERR_MISSING_START_OFFSET)
	ErrBuildingOffsets    = errors.NewAppError(ERR_BUILDING_OFFSETS)
	ErrMissingOffsets     = errors.NewAppError(ERR_MISSING_OFFSETS)
	ErrMissingValue       = errors.NewAppError(ERR_MISSING_VALUE)
	ErrAddingAgent        = errors.NewAppError(ERR_ADDING_AGENT)
	ErrAddingPriciipal    = errors.NewAppError(ERR_ADDING_PRINCIPAL)
	ErrAddingFiling       = errors.NewAppError(ERR_ADDING_FILING)
	ErrUnknownEntity      = errors.NewAppError(ERR_UNKNOW_ENTITY_TYPE)
)

const DataPathContextKey = clients.ContextKey("data-path")

const DEFAULT_DATA_PATH = "data"
const DEFAULT_BATCH_SIZE int64 = 400
const RECORD_SIZE int64 = 90

type CSVActivityFn = func(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error)

// AddAgentActivity returns successfully added agent's Id or error.
func AddAgentActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	// l.Info("AddAgentActivity - started", zap.Any("fields", fields))

	schClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(common.ERR_MISSING_SCHEDULER_CLIENT)
		return "", cadence.NewCustomError(common.ERR_MISSING_SCHEDULER_CLIENT, common.ErrMissingSchClient)
	}

	resp, err := schClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	if err != nil {
		l.Error(ERR_ADDING_AGENT, zap.Error(err))
		return "", cadence.NewCustomError(ERR_ADDING_AGENT, err)
	}

	agent := resp.GetAgent()
	// l.Info("AddAgentActivity - agent", zap.Any("agent", agent))

	return agent.Id, nil
}

// AddPrincipalActivity returns successfully added principal's Id or error.
func AddPrincipalActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	// l.Info("AddPrincipalActivity - started", zap.Any("fields", fields))

	bizClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(common.ERR_MISSING_SCHEDULER_CLIENT)
		return "", cadence.NewCustomError(common.ERR_MISSING_SCHEDULER_CLIENT, common.ErrMissingSchClient)
	}

	resp, err := bizClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	if err != nil {
		l.Error(ERR_ADDING_PRINCIPAL, zap.Error(err))
		return "", cadence.NewCustomError(ERR_ADDING_PRINCIPAL, err)
	}

	princp := resp.GetPrincipal()
	// l.Info("AddPrincipalActivity - agent", zap.Any("agent", agent))

	return princp.Id, nil
}

// AddFilingActivity returns successfully added filing's Id or error.
func AddFilingActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	// l.Info("AddFilingActivity - started", zap.Any("fields", fields))

	bizClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(common.ERR_MISSING_SCHEDULER_CLIENT)
		return "", cadence.NewCustomError(common.ERR_MISSING_SCHEDULER_CLIENT, common.ErrMissingSchClient)
	}

	resp, err := bizClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_FILING,
	})
	if err != nil {
		l.Error(ERR_ADDING_FILING, zap.Error(err))
		return "", cadence.NewCustomError(ERR_ADDING_FILING, err)
	}

	fil := resp.GetFiling()
	// l.Info("AddFilingActivity - agent", zap.Any("agent", agent))

	return fil.Id, nil
}

// GetCSVHeadersActivity returns CSV state populated with headers info &
// records starting index for given file. Requires same host once processing starts,
// resolves file path using configured/default data directory path
func GetCSVHeadersActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("hostID", HostID))
	l.Debug("GetCSVHeadersActivity - started", zap.String("file", req.FileName))

	// return if headers already build
	if req.Headers != nil {
		return req, nil
	}

	// check for same host
	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	if hostId != HostID {
		l.Error("GetCSVHeadersActivity - running on wrong host",
			zap.String("file", req.FileName),
			zap.String("req-host", hostId),
			zap.String("curr-host", HostID))

		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	// check for file name
	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	// check for requester
	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return req, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	// build local file path
	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = DEFAULT_DATA_PATH
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	// open file
	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(common.ERR_MISSING_FILE, zap.Error(err), zap.String("file", req.FileName), zap.String("host", hostId))
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", zap.Error(err))
		}
	}()

	// persist file size
	fs, err := os.Stat(file.Name())
	if err != nil {
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE, err)
	}
	req.FileSize = int64(fs.Size())

	csvReader := csv.NewReader(file)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err != nil {
		l.Error(ERR_CSV_READ, zap.Error(err), zap.String("file", req.FileName), zap.String("host", hostId))
		return req, cadence.NewCustomError(ERR_CSV_READ, err)
	}
	offset := csvReader.InputOffset()
	req.Headers = &models.HeadersInfo{
		Headers: headers,
		Offset:  offset,
	}
	return req, nil
}

// GetCSVOffsetsActivity populates CSV state with partition offsets for given file.
func GetCSVOffsetsActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("hostID", HostID))
	l.Info("GetCSVOffsetsActivity - started", zap.String("file", req.FileName), zap.Int64("size", req.FileSize))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// l.Info("GetCSVOffsetsActivity", zap.String("hostId", hostId), zap.String("HostID", HostID))

	// check for same host
	if hostId != HostID {
		l.Error("GetCSVHeadersActivity - running on wrong host",
			zap.String("file", req.FileName),
			zap.String("req-host", hostId),
			zap.String("curr-host", HostID))

		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return req, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = DEFAULT_DATA_PATH
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(common.ERR_MISSING_FILE, zap.Error(err), zap.String("file", req.FileName), zap.String("host", hostId))
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", zap.Error(err))
		}
	}()

	if req.Headers == nil {
		l.Error(ERR_MISSING_START_OFFSET)
		return req, cadence.NewCustomError(ERR_MISSING_START_OFFSET, ErrMissingStartOffset)
	}

	if req.FileSize == 0 {
		fs, err := os.Stat(file.Name())
		if err != nil {
			l.Error(common.ERR_MISSING_FILE, zap.Error(err), zap.String("file", req.FileName), zap.String("host", hostId))
			return req, cadence.NewCustomError(common.ERR_MISSING_FILE, err)
		}
		req.FileSize = fs.Size()
	}

	offsets := []int64{req.Headers.Offset}
	if req.FileSize/DEFAULT_BATCH_SIZE < 2 {
		req.OffSets = offsets
		return req, nil
	}
	l.Info(
		"GetCSVOffsetsActivity",
		zap.String("file", req.FileName),
		zap.Any("file-size", req.FileSize),
		zap.Any("batch-size", DEFAULT_BATCH_SIZE),
		zap.Any("record-size", RECORD_SIZE),
	)
	sOffset := req.Headers.Offset
	for sOffset+DEFAULT_BATCH_SIZE+RECORD_SIZE < req.FileSize {
		sOffset = sOffset + DEFAULT_BATCH_SIZE
		nextOffset, err := getNextOffset(file, sOffset)
		if err != nil {
			l.Error(ERR_BUILDING_OFFSETS, zap.Error(err), zap.String("file", req.FileName), zap.String("host", hostId))
			return req, cadence.NewCustomError(ERR_BUILDING_OFFSETS, err)
		}
		l.Info(
			"GetCSVOffsetsActivity - nextOffset",
			zap.String("file", req.FileName),
			zap.Any("read-offset", sOffset+RECORD_SIZE),
			zap.Any("next-offset", nextOffset),
		)
		offsets = append(offsets, nextOffset)
		sOffset = nextOffset
	}

	req.OffSets = offsets
	return req, nil
}

// ReadCSVActivity reads csv file and adds business entities to scheduler data store.
func ReadCSVActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("hostID", HostID))
	l.Info("ReadCSVActivity - started", zap.String("file", req.FileName), zap.Int64("size", req.FileSize))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	l.Info("ReadCSVActivity", zap.String("hostId", hostId), zap.String("HostID", HostID))

	// check for same host
	if hostId != HostID {
		l.Error("GetCSVHeadersActivity - running on wrong host",
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

	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	if len(req.OffSets) == 0 {
		l.Error(ERR_MISSING_OFFSETS)
		return req, cadence.NewCustomError(ERR_MISSING_OFFSETS, ErrMissingOffsets)
	}

	schClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(common.ERR_MISSING_SCHEDULER_CLIENT)
		return nil, cadence.NewCustomError(common.ERR_MISSING_SCHEDULER_CLIENT, common.ErrMissingSchClient)
	}

	resCh := make(chan []string)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var count, resultCount, errCount int

	go ReadRecords(ctx, localFilePath, resCh, errCh, req.FileSize, req.OffSets, l)

	errs := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			l.Info(
				"ReadCSVActivity - file processing done, context close",
				zap.Int("batches", len(req.OffSets)),
				zap.Int("count", count),
				zap.Int("resultCount", resultCount),
				zap.Int("errCount", errCount))
			if len(errs) > 0 {
				for k, v := range errs {
					l.Info(
						"ReadCSVActivity - file processing done, context closed",
						zap.String("err", k),
						zap.Int("errCount", v))
				}
			}
			return req, nil
		case r, ok := <-resCh:
			if !ok {
				l.Info(
					"ReadCSVActivity - result stream closed",
					zap.Int("batches", len(req.OffSets)),
					zap.Int("count", count),
					zap.Int("resultCount", resultCount),
					zap.Int("errCount", errCount))
				if len(errs) > 0 {
					for k, v := range errs {
						l.Info(
							"ReadCSVActivity - result stream closed",
							zap.String("err", k),
							zap.Int("errCount", v))
					}
				}
				return req, nil
			} else {
				if r != nil {
					count++

					if fields, err := mapToInterface(req.Headers.Headers, r); err == nil {
						_, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
						if !ok {
							l.Error(common.ERR_MISSING_SCHEDULER_CLIENT)
							errCount++
							errs[common.ERR_MISSING_SCHEDULER_CLIENT]++
						} else {
							l.Info("adding business entity", zap.Any("type", req.Type), zap.Any("fields", fields))
							if _, err := schClient.AddEntity(ctx, &api.AddEntityRequest{
								Fields: fields,
								Type:   models.MapEntityTypeToProto(req.Type),
							}); err == nil {
								// l.Info("business entity added", zap.Any("type", req.Type), zap.Any("agent", resp.GetAgent()))
								resultCount++
							} else {
								errCount++
								errs[err.Error()]++
							}
						}
					} else {
						errCount++
						errs[err.Error()]++
					}
				}
			}
		case err, ok := <-errCh:
			if !ok {
				l.Info(
					"ReadCSVActivity - error stream closed",
					zap.Int("batches", len(req.OffSets)),
					zap.Int("count", count),
					zap.Int("resultCount", resultCount),
					zap.Int("errCount", errCount))
				if len(errs) > 0 {
					for k, v := range errs {
						l.Info(
							"ReadCSVActivity - error stream closed",
							zap.String("err", k),
							zap.Int("errCount", v))
					}
				}
				return req, nil
			} else {
				if err != nil {
					errCount++
					errs[err.Error()]++
				}
			}
		}
	}
}

// ReadFile reads csv file, sends records to result channel and errors to error channel.
func ReadFile(ctx context.Context, filePath string, resCh chan []string, errCh chan error, l logger.AppLogger) {
	defer func() {
		close(resCh)
		close(errCh)
	}()

	file, err := os.Open(filePath)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", zap.Error(err))
		}
	}()

	csvReader := csv.NewReader(file)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	offset := csvReader.InputOffset()
	l.Info("starting offset", zap.Any("offset", offset))
	for i := 0; ; i = i + 1 {
		record, err := csvReader.Read()
		if err == io.EOF {
			l.Info("csv file: end of csv file")
			return
		} else if err != nil {
			size := csvReader.InputOffset() - offset
			l.Info("error reading csv record", zap.Error(err), zap.Any("offset", offset), zap.Any("size", size))
			errCh <- errors.WrapError(err, "error reading csv record, offset: %d, size: %d", offset, size)
		}
		offset = csvReader.InputOffset()
		select {
		case <-ctx.Done():
			return
		case resCh <- record:
		}
	}
}

// ReadRecords concurrently reads partitons of csv file for the calculated offsets,
// manages number of concurrents reads based on batchBuffer,
// sends records to result channel and errors to error channel.
func ReadRecords(
	ctx context.Context,
	filePath string,
	resCh chan []string,
	errCh chan error,
	size int64,
	offsets []int64,
	l logger.AppLogger,
) {
	defer func() {
		close(resCh)
		close(errCh)
	}()

	var wg sync.WaitGroup
	batchCount := 0
	batchBuffer := 3
	for i, offset := range offsets {
		batchCount++
		if i >= len(offsets)-1 {
			l.Info("batch info", zap.Any("start", offset), zap.Any("end", size), zap.Any("batchIdx", i))
			wg.Add(1)
			go readRecords(ctx, filePath, resCh, errCh, &wg, offset, size, l)
		} else {
			l.Info("batch info", zap.Any("start", offset), zap.Any("end", offsets[i+1]), zap.Any("batchIdx", i))
			wg.Add(1)
			go readRecords(ctx, filePath, resCh, errCh, &wg, offset, offsets[i+1], l)
		}
		if batchCount >= batchBuffer {
			batchCount = 0
			if len(offsets)-i < batchBuffer {
				batchBuffer = len(offsets) - i
				l.Info("last batch info", zap.Any("batch-size", batchBuffer), zap.Any("record", i))
			}
			wg.Wait()
		}
	}
	wg.Wait()
}

// ReadRecord reads single record for given file path, offset and size
func ReadRecord(
	filePath string,
	offset, size int64,
	l logger.AppLogger,
) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", zap.Error(err))
		}
	}()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		l.Error("error", zap.Error(err))
		return "", err
	}
	return string(data[:n]), nil
}

// readRecords reads records from given start and end offsets partition of csv file
func readRecords(
	ctx context.Context,
	filePath string,
	resCh chan []string,
	errCh chan error,
	wg *sync.WaitGroup,
	start, end int64,
	l logger.AppLogger,
) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		err := file.Close()
		if err != nil && err != os.ErrClosed {
			l.Error("error closing csv file", zap.Error(err))
		}
	}()

	data := make([]byte, end-start)
	_, err = file.ReadAt(data, start)
	if err != nil {
		l.Error("error", zap.Error(err))
		errCh <- err
		return
	}

	bReader := bytes.NewReader(data)
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1
	// csvReader.LazyQuotes = true

	resultCount := 0
	errCount := 0

	offset := csvReader.InputOffset()
	for i := 0; ; i = i + 1 {
		record, err := csvReader.Read()
		if err == io.EOF {
			l.Info("batch done", zap.Any("start", start), zap.Any("end", end))
			return
		} else if err != nil {
			size := csvReader.InputOffset() - offset
			var singleRec []string
			if recStr, err := ReadRecord(filePath, start+offset, size, l); err != nil {
				l.Error("error reading error record", zap.Error(err))
			} else {
				recStr = strings.Replace(recStr, "\"", "", -1)
				singleRec, err = readSingleRecord(recStr)
				if err != nil {
					l.Error("error reading single record", zap.Error(err))
				}
			}
			// l.Info("single record", zap.Any("singleRec", singleRec), zap.Any("offset", start+offset), zap.Any("size", size))
			if len(singleRec) > 0 {
				resultCount++
				resCh <- singleRec
			} else {
				l.Error("error reading csv record", zap.Error(err), zap.Any("offset", start+offset), zap.Any("size", size))
				errCount++
				errCh <- errors.WrapError(err, "error reading csv record")
			}
		}
		// l.Info("record", zap.Any("record", record))
		offset = csvReader.InputOffset()
		select {
		case <-ctx.Done():
			return
		case resCh <- record:
			resultCount++
		}
	}
}

// readSingleRecord reads single record from given data byte string
func readSingleRecord(recStr string) ([]string, error) {
	bReader := bytes.NewReader([]byte(recStr))
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	record, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	return record, nil
}

// mapToInterface maps headers and values to map[string]string
func mapToInterface(headers, values []string) (map[string]string, error) {
	if len(headers) != len(values) {
		return nil, ErrMissingValue
	}

	val := map[string]string{}
	for i, k := range headers {
		val[strings.ToLower(k)] = values[i]
	}

	// fmt.Printf("business entity mapping - value: %v, val: %v\n", values, val)
	return val, nil
}

// getNextOffset returns next record offset
func getNextOffset(file *os.File, offset int64) (int64, error) {
	data := make([]byte, RECORD_SIZE)
	_, err := file.ReadAt(data, int64(offset))
	if err != nil {
		return offset, errors.WrapError(err, ERR_CSV_READ)
	}

	var nextOffset int64
	i := bytes.IndexAny(data, "\r\n")
	if i >= 0 {
		nextOffset = offset + int64(i) + 1
	} else {
		nextOffset, err = getNextOffset(file, offset+RECORD_SIZE)
	}

	if err != nil {
		return offset, err
	}
	return nextOffset, nil
}
