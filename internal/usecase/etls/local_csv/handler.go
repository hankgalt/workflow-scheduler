package localcsv

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/etls"
	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

type LocalCSVFileHandlerConfig struct {
	name, path string // Name & Path of the CSV file
}

func NewLocalCSVFileHandlerConfig(name, path string) LocalCSVFileHandlerConfig {
	return LocalCSVFileHandlerConfig{name: name, path: path}
}

func (c LocalCSVFileHandlerConfig) Name() string { return c.name }
func (c LocalCSVFileHandlerConfig) Path() string { return c.path }

type LocalCSVFileHandler struct {
	dataPoint batch.CSVDataPoint
	headers   []string
}

func NewLocalCSVFileHandler(cfg LocalCSVFileHandlerConfig) (*LocalCSVFileHandler, error) {
	if cfg.Name() == "" {
		return nil, etls.ErrMissingFileName
	}

	return &LocalCSVFileHandler{
		dataPoint: batch.CSVDataPoint{Name: cfg.Name(), Path: cfg.Path()},
	}, nil
}

func (h *LocalCSVFileHandler) Headers() []string {
	if h.headers == nil {
		return []string{} // Return empty slice if headers are not set
	}
	return h.headers
}

// ReadData reads data from a local CSV file at the specified offset and limit.
// It returns the data read, the next offset to read from, a boolean indicating if EOF was reached,
// and any error encountered during the read operation.
func (h *LocalCSVFileHandler) ReadData(ctx context.Context, offset, limit uint64) (any, uint64, bool, error) {
	// Construct the local file path using the data point's path and name
	localFilePath := filepath.Join(h.dataPoint.Path, h.dataPoint.Name)

	// Open the file for reading
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, 0, false, fmt.Errorf("error opening file %s: %w", localFilePath, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("error closing file %s: %v\n", localFilePath, err)
		}
	}()

	// Check if the offset is beyond the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, 0, false, fmt.Errorf("error getting file info for %s: %w", localFilePath, err)
	}
	if offset >= uint64(fileInfo.Size()) {
		return nil, 0, true, fmt.Errorf("offset %d is beyond the file size %d for file %s", offset, fileInfo.Size(), localFilePath)
	}

	// Read data bytes from the file at the specified offset
	data := make([]byte, limit)
	n, err := file.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return nil, 0, false, fmt.Errorf("error reading file %s at offset %d: %w", localFilePath, offset, err)
	}

	// If offset is less than 1, we assume it's the first read and we can read csv headers
	startOffset := 0
	if offset < 1 {
		buffer := bytes.NewBuffer(data)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

		headers, err := csvReader.Read()
		if err != nil {
			return nil, 0, false, fmt.Errorf("error reading headers from file %s: %w", localFilePath, err)
		}
		// Store the headers in the handler
		h.headers = strutils.CleanHeaders(headers)
		// set the start offset to first record start
		startOffset = int(csvReader.InputOffset())
	}

	var nextOffset uint64
	i := bytes.LastIndex(data, []byte{'\n'})
	if i > 0 && n == int(limit) {
		// If we found a newline, we can assume the next read should start after this
		nextOffset = uint64(i) + 1
	} else {
		nextOffset = uint64(n) // If no newline found, we read till the end
	}

	return data[startOffset:nextOffset], offset + nextOffset, n < int(limit), nil
}

// HandleData processes the data read from the CSV file.
// It reads the CSV records from the byte slice and returns a channel for results and errors.
func (h *LocalCSVFileHandler) HandleData(
	ctx context.Context,
	start uint64,
	data any,
	transFunc batch.TransformerFunc,
) (<-chan batch.Result, error) {
	chnk, ok := data.([]byte)
	if !ok {
		return nil, errors.New("invalid data type, expected []byte")
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, err
	}

	resStream := make(chan batch.Result)
	go func() {
		defer close(resStream)

		buffer := bytes.NewBuffer(chnk)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

		currOffset := csvReader.InputOffset()
		lastOffset := currOffset
		for {
			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					l.Debug("reached EOF, ending go routine. Last record", "start", lastOffset, "end", currOffset)
					return
				} else {
					cleanedStr := strutils.CleanRecord(string(chnk[currOffset:csvReader.InputOffset()]))
					record, err = strutils.ReadSingleRecord(cleanedStr)
					if err != nil {
						resStream <- batch.Result{
							Start: start + uint64(lastOffset),
							End:   start + uint64(currOffset),
							Error: err.Error(),
						}
						continue
					}
				}
			}

			lastOffset, currOffset = currOffset, csvReader.InputOffset()
			cleanedRec := strutils.CleanAlphaNumericsArr(record, []rune{'.', '-', '_', '#', '&', '@'})
			resStream <- batch.Result{
				Start:  start + uint64(lastOffset),
				End:    start + uint64(currOffset),
				Record: cleanedRec,
			}
		}
	}()
	return resStream, nil
}
