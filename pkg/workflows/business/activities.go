package business

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/comfforts/errors"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

/**
 * activities used by file processing workflow.
 */
const (
	AddAgentActivityName      = "AddAgentActivity"
	AddPrincipalActivityName  = "AddPrincipalActivity"
	AddFilingActivityName     = "AddFilingActivity"
	GetCSVHeadersActivityName = "GetCSVHeadersActivity"
	GetCSVOffsetsActivityName = "GetCSVOffsetsActivity"
	ReadCSVActivityName       = "ReadCSVActivity"
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
)

var (
	ErrMissingStartOffset = errors.NewAppError(ERR_MISSING_START_OFFSET)
	ErrBuildingOffsets    = errors.NewAppError(ERR_BUILDING_OFFSETS)
	ErrMissingOffsets     = errors.NewAppError(ERR_MISSING_OFFSETS)
	ErrMissingValue       = errors.NewAppError(ERR_MISSING_VALUE)
	ErrAddingAgent        = errors.NewAppError(ERR_ADDING_AGENT)
	ErrAddingPriciipal    = errors.NewAppError(ERR_ADDING_PRINCIPAL)
	ErrAddingFiling       = errors.NewAppError(ERR_ADDING_FILING)
)

var (
	ErrorMissingStartOffset = temporal.NewApplicationErrorWithCause(ERR_MISSING_START_OFFSET, ERR_MISSING_START_OFFSET, ErrMissingStartOffset)
	ErrorMissingOffsets     = temporal.NewApplicationErrorWithCause(ERR_MISSING_OFFSETS, ERR_MISSING_OFFSETS, ErrMissingOffsets)
)

const DataPathContextKey = clients.ContextKey("data-path")

const DEFAULT_DATA_PATH = "data"
const MIN_BATCH_SIZE int64 = 400
const MAX_BATCH_SIZE int64 = 4000

type CSVActivityFn = func(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error)

// AddAgentActivity returns successfully added agent's Id or error.
func AddAgentActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx)
	l.Info("AddAgentActivity - started", slog.Any("fields", fields))

	schClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(comwkfl.ERR_MISSING_SCHEDULER_CLIENT)
		return "", comwkfl.ErrorMissingSchedulerClient
	}

	resp, err := schClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	if err != nil {
		l.Error(ERR_ADDING_AGENT, slog.String("error", err.Error()))
		return "", temporal.NewApplicationErrorWithCause(ERR_ADDING_AGENT, ERR_ADDING_AGENT, errors.WrapError(err, ERR_ADDING_AGENT))
	}

	agent := resp.GetAgent()
	l.Info("AddAgentActivity - agent", slog.Any("agent", agent))

	return agent.Id, nil
}

// AddPrincipalActivity returns successfully added principal's Id or error.
func AddPrincipalActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx)
	l.Info("AddPrincipalActivity - started", slog.Any("fields", fields))

	bizClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(comwkfl.ERR_MISSING_SCHEDULER_CLIENT)
		return "", comwkfl.ErrorMissingSchedulerClient
	}

	resp, err := bizClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	if err != nil {
		l.Error(ERR_ADDING_PRINCIPAL, slog.String("error", err.Error()))
		return "", temporal.NewApplicationErrorWithCause(ERR_ADDING_PRINCIPAL, ERR_ADDING_PRINCIPAL, errors.WrapError(err, ERR_ADDING_PRINCIPAL))
	}

	princp := resp.GetPrincipal()
	l.Info("AddPrincipalActivity - principal", slog.Any("principal", princp))

	return princp.Id, nil
}

// AddFilingActivity returns successfully added filing's Id or error.
func AddFilingActivity(ctx context.Context, fields map[string]string) (string, error) {
	l := activity.GetLogger(ctx)
	l.Info("AddFilingActivity - started", slog.Any("fields", fields))

	bizClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(comwkfl.ERR_MISSING_SCHEDULER_CLIENT)
		return "", comwkfl.ErrorMissingSchedulerClient
	}

	resp, err := bizClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_FILING,
	})
	if err != nil {
		l.Error(ERR_ADDING_FILING, slog.String("error", err.Error()))
		return "", temporal.NewApplicationErrorWithCause(ERR_ADDING_FILING, ERR_ADDING_FILING, errors.WrapError(err, ERR_ADDING_FILING))
	}

	fil := resp.GetFiling()
	l.Info("AddFilingActivity - filing", slog.Any("filing", fil))

	return fil.Id, nil
}

// GetCSVHeadersActivity returns CSV state populated with headers info &
// records starting index for given file. Requires same host once processing starts,
// resolves file path using configured/default data directory path
func GetCSVHeadersActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx)
	l.Debug("GetCSVHeadersActivity - started", slog.String("file-name", req.FileName))

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
			slog.String("file", req.FileName),
			slog.String("req-host", hostId),
			slog.String("curr-host", HostID))

		return req, comwkfl.ErrorWrongHost
	}

	// check for file name
	if req.FileName == "" {
		l.Error(comwkfl.ERR_MISSING_FILE_NAME)
		return req, comwkfl.ErrorMissingFileName
	}

	// check for requester
	if req.RequestedBy == "" {
		l.Error(comwkfl.ERR_MISSING_REQSTR)
		return req, comwkfl.ErrorMissingReqstr
	}

	// build local file path
	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = DEFAULT_DATA_PATH
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	l.Debug("GetCSVHeadersActivity - local file path", slog.String("file-path", localFilePath))

	// open file
	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(comwkfl.ERR_MISSING_FILE, slog.Any("error", err), slog.String("file", req.FileName), slog.String("host", hostId))
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_MISSING_FILE, comwkfl.ERR_MISSING_FILE, errors.WrapError(err, comwkfl.ERR_MISSING_FILE))
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", slog.Any("error", err))
		}
	}()

	// persist file size
	fs, err := os.Stat(file.Name())
	if err != nil {
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_MISSING_FILE, comwkfl.ERR_MISSING_FILE, errors.WrapError(err, comwkfl.ERR_MISSING_FILE))
	}
	req.FileSize = int64(fs.Size())

	csvReader := csv.NewReader(file)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err != nil {
		l.Error(ERR_CSV_READ, slog.Any("error", err), slog.String("file", req.FileName), slog.String("host", hostId))
		return req, temporal.NewApplicationErrorWithCause(ERR_CSV_READ, ERR_CSV_READ, errors.WrapError(err, ERR_CSV_READ))
	}
	offset := csvReader.InputOffset()
	req.Headers = &models.HeadersInfo{
		Headers: headers,
		Offset:  offset,
	}

	aveRecordSize := offset
	samples := 0
	allRead := false

	for samples < 5 || !allRead {
		_, err := csvReader.Read()
		if err == io.EOF {
			allRead = true
		} else if err != nil {
			samples = 5
			allRead = true
		} else {
			newOffset := csvReader.InputOffset()
			aveRecordSize = (aveRecordSize + (newOffset - offset)) / 2
			offset = newOffset
		}
		samples++
	}
	req.AvgRecordSize = aveRecordSize
	return req, nil
}

// GetCSVOffsetsActivity populates CSV state with partition offsets for given file.
func GetCSVOffsetsActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx)
	l.Info("GetCSVOffsetsActivity - started", slog.String("file", req.FileName), slog.Int64("size", req.FileSize))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// l.Info("GetCSVOffsetsActivity", slog.String("hostId", hostId), slog.String("HostID", HostID))

	// check for same host
	if hostId != HostID {
		l.Error("GetCSVHeadersActivity - running on wrong host",
			slog.String("file", req.FileName),
			slog.String("req-host", hostId),
			slog.String("curr-host", HostID))

		return req, comwkfl.ErrorWrongHost
	}

	if req.FileName == "" {
		l.Error(comwkfl.ERR_MISSING_FILE_NAME)
		return req, comwkfl.ErrorMissingFileName
	}

	if req.RequestedBy == "" {
		l.Error(comwkfl.ERR_MISSING_REQSTR)
		return req, comwkfl.ErrorMissingReqstr
	}

	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = DEFAULT_DATA_PATH
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	file, err := os.Open(localFilePath)
	if err != nil {
		l.Error(comwkfl.ERR_MISSING_FILE, slog.Any("error", err), slog.String("file", req.FileName), slog.String("host", hostId))
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_MISSING_FILE, comwkfl.ERR_MISSING_FILE, errors.WrapError(err, comwkfl.ERR_MISSING_FILE))
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", slog.Any("error", err))
		}
	}()

	if req.Headers == nil {
		l.Error(ERR_MISSING_START_OFFSET)
		return req, ErrorMissingStartOffset
	}

	if req.FileSize == 0 {
		fs, err := os.Stat(file.Name())
		if err != nil {
			l.Error(comwkfl.ERR_MISSING_FILE, slog.Any("error", err), slog.String("file", req.FileName), slog.String("host", hostId))
			return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_MISSING_FILE, comwkfl.ERR_MISSING_FILE, errors.WrapError(err, comwkfl.ERR_MISSING_FILE))
		}
		req.FileSize = fs.Size()
	}

	// setup batch size
	batchSize := MIN_BATCH_SIZE
	if req.BatchSize <= 0 {
		if req.FileSize > MAX_BATCH_SIZE+req.AvgRecordSize {
			req.BatchSize = MAX_BATCH_SIZE
		}
	} else {
		batchSize = req.BatchSize
	}

	offsets := []int64{req.Headers.Offset}
	if req.FileSize/batchSize < 2 {
		req.OffSets = offsets
		return req, nil
	}
	l.Info(
		"GetCSVOffsetsActivity",
		slog.String("file", req.FileName),
		slog.Any("file-size", req.FileSize),
		slog.Any("batch-size", batchSize),
		slog.Int64("record-size", req.AvgRecordSize),
	)
	sOffset := req.Headers.Offset
	for sOffset+batchSize+req.AvgRecordSize < req.FileSize {
		ofSet := sOffset
		sOffset = sOffset + batchSize
		nextOffset, err := getNextOffset(file, sOffset, req.AvgRecordSize)
		if err != nil {
			l.Error(ERR_BUILDING_OFFSETS, slog.Any("error", err), slog.String("file", req.FileName), slog.String("host", hostId))
			return req, temporal.NewApplicationErrorWithCause(ERR_BUILDING_OFFSETS, ERR_BUILDING_OFFSETS, errors.WrapError(err, ERR_BUILDING_OFFSETS))
		}
		l.Debug(
			"GetCSVOffsetsActivity - nextOffset",
			slog.String("file", req.FileName),
			slog.Any("start-offset", ofSet),
			slog.Any("next-offset", nextOffset),
			slog.Any("numOffsets", len(offsets)),
		)

		offsets = append(offsets, nextOffset)
		sOffset = nextOffset
	}

	req.OffSets = offsets
	return req, nil
}

// ReadCSVActivity reads csv file and adds business entities to scheduler data store.
func ReadCSVActivity(ctx context.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := activity.GetLogger(ctx)
	l.Info("ReadCSVActivity - started", slog.String("file", req.FileName), slog.Int64("size", req.FileSize))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	l.Info("ReadCSVActivity", slog.String("hostId", hostId), slog.String("HostID", HostID))

	// check for same host
	if hostId != HostID {
		l.Error("GetCSVHeadersActivity - running on wrong host",
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

	dataPath, ok := ctx.Value(DataPathContextKey).(string)
	if !ok || dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	if len(req.OffSets) == 0 {
		l.Error(ERR_MISSING_OFFSETS)
		return req, ErrorMissingOffsets
	}

	schClient, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
	if !ok {
		l.Error(comwkfl.ERR_MISSING_SCHEDULER_CLIENT)
		return nil, comwkfl.ErrorMissingSchedulerClient
	}

	resCh := make(chan []string)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var count, resultCount, errCount int

	go ReadRecords(ctx, localFilePath, resCh, errCh, req.FileSize, req.OffSets)

	errs := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			l.Info(
				"ReadCSVActivity - file processing done, context close",
				slog.Int("batches", len(req.OffSets)),
				slog.Int("count", count),
				slog.Int("resultCount", resultCount),
				slog.Int("errCount", errCount))
			if len(errs) > 0 {
				for k, v := range errs {
					l.Info(
						"ReadCSVActivity - file processing done, context closed",
						slog.String("err", k),
						slog.Int("errCount", v))
				}
			}
			return req, nil
		case r, ok := <-resCh:
			if !ok {
				l.Info(
					"ReadCSVActivity - result stream closed",
					slog.Int("batches", len(req.OffSets)),
					slog.Int("count", count),
					slog.Int("resultCount", resultCount),
					slog.Int("errCount", errCount))
				if len(errs) > 0 {
					for k, v := range errs {
						l.Info(
							"ReadCSVActivity - result stream closed",
							slog.String("err", k),
							slog.Int("errCount", v))
					}
				}
				return req, nil
			} else {
				if r != nil {
					count++

					if fields, err := mapToInterface(req.Headers.Headers, r); err == nil {
						_, ok := ctx.Value(scheduler.SchedulerClientContextKey).(scheduler.Client)
						if !ok {
							l.Error(comwkfl.ERR_MISSING_SCHEDULER_CLIENT)
							errCount++
							errs[comwkfl.ERR_MISSING_SCHEDULER_CLIENT]++
						} else {
							l.Info("adding business entity", slog.Any("type", req.Type), slog.Any("fields", fields))
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
					slog.Int("batches", len(req.OffSets)),
					slog.Int("count", count),
					slog.Int("resultCount", resultCount),
					slog.Int("errCount", errCount))
				if len(errs) > 0 {
					for k, v := range errs {
						l.Info(
							"ReadCSVActivity - error stream closed",
							slog.String("err", k),
							slog.Int("errCount", v))
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
) {
	defer func() {
		close(resCh)
		close(errCh)
	}()

	l := activity.GetLogger(ctx)

	var wg sync.WaitGroup
	batchCount := 0
	batchBuffer := 3
	for i, offset := range offsets {
		batchCount++
		if i >= len(offsets)-1 {
			l.Info("batch info", slog.Any("start", offset), slog.Any("end", size), slog.Any("batchIdx", i))
			wg.Add(1)
			go readRecords(ctx, filePath, resCh, errCh, &wg, offset, size)
		} else {
			l.Info("batch info", slog.Any("start", offset), slog.Any("end", offsets[i+1]), slog.Any("batchIdx", i))
			wg.Add(1)
			go readRecords(ctx, filePath, resCh, errCh, &wg, offset, offsets[i+1])
		}
		if batchCount >= batchBuffer {
			batchCount = 0
			if len(offsets)-i < batchBuffer {
				batchBuffer = len(offsets) - i
				l.Info("last batch info", slog.Any("batch-size", batchBuffer), slog.Any("record", i))
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
) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			slog.Error("error closing csv file", slog.Any("error", err))
		}
	}()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil {
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
) {
	defer wg.Done()

	l := activity.GetLogger(ctx)

	file, err := os.Open(filePath)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		err := file.Close()
		if err != nil && err != os.ErrClosed {
			l.Error("error closing csv file", slog.Any("error", err))
		}
	}()

	data := make([]byte, end-start)
	_, err = file.ReadAt(data, start)
	if err != nil {
		l.Error("error", slog.Any("error", err))
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
			l.Info("batch done", slog.Any("start", start), slog.Any("end", end))
			return
		} else if err != nil {
			size := csvReader.InputOffset() - offset
			var singleRec []string
			if recStr, err := readRecord(ctx, filePath, start+offset, size); err != nil {
				l.Error("error reading error record", slog.Any("error", err))
			} else {
				recStr = strings.Replace(recStr, "\"", "", -1)
				singleRec, err = readSingleRecord(recStr)
				if err != nil {
					l.Error("error reading single record", slog.Any("error", err))
				}
			}
			// l.Info("single record", zap.Any("singleRec", singleRec), zap.Any("offset", start+offset), zap.Any("size", size))
			if len(singleRec) > 0 {
				resultCount++
				resCh <- singleRec
			} else {
				l.Error("error reading csv record", slog.Any("error", err), slog.Any("offset", start+offset), slog.Any("size", size))
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

// readRecord reads single record for given file path, offset and size
func readRecord(
	ctx context.Context,
	filePath string,
	offset, size int64,
) (string, error) {
	l := activity.GetLogger(ctx)

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing csv file", slog.Any("error", err))
		}
	}()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		l.Error("error", slog.Any("error", err))
		return "", err
	}
	return string(data[:n]), nil
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

// getNextOffset returns next record offset
func getNextOffset(file *os.File, offset, avRecordSize int64) (int64, error) {
	data := make([]byte, avRecordSize)
	_, err := file.ReadAt(data, int64(offset))
	if err != nil {
		return offset, errors.WrapError(err, ERR_CSV_READ)
	}

	var nextOffset int64
	i := bytes.IndexAny(data, "\r\n")
	if i >= 0 {
		nextOffset = offset + int64(i) + 1
	} else {
		nextOffset, err = getNextOffset(file, offset+avRecordSize, avRecordSize)
	}

	if err != nil {
		return offset, err
	}
	return nextOffset, nil
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
