package sources

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"

	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const LocalCSVSource = "local-csv-source"

const (
	ERR_LOCAL_CSV_PATH_REQUIRED   = "local csv: path is required"
	ERR_LOCAL_CSV_TRANSFORMER_NIL = "local csv: transformer function is not set for local CSV source with headers"
	ERR_LOCAL_CSV_FILE_OPEN       = "local csv: open"
	ERR_LOCAL_CSV_SIZE_INVALID    = "local csv: size must be greater than 0"
	ERR_INVALID_OFFSET            = "local csv: invalid offset, must be string"
	ERR_PARSING_OFFSET            = "local csv: error parsing offset, must be int64"
)

var (
	ErrLocalCSVPathRequired = errors.New(ERR_LOCAL_CSV_PATH_REQUIRED)
	ErrLocalCSVFileOpen     = errors.New(ERR_LOCAL_CSV_FILE_OPEN)
	ErrLocalCSVSizeInvalid  = errors.New(ERR_LOCAL_CSV_SIZE_INVALID)
	ErrNilTransformer       = errors.New(ERR_LOCAL_CSV_TRANSFORMER_NIL)
	ErrInvalidOffset        = errors.New(ERR_INVALID_OFFSET)
	ErrParsingOffset        = errors.New(ERR_PARSING_OFFSET)
)

// Local CSV source.
type localCSVSource struct {
	path      string
	size      int64
	delimiter rune
	hasHeader bool
	transFunc domain.TransformerFunc // transformer function to apply to each row
}

// Name of the source.
func (s *localCSVSource) Name() string { return LocalCSVSource }

// Close closes the local CSV source.
func (s *localCSVSource) Close(ctx context.Context) error {
	// No resources to close for local CSV source
	return nil
}

// Size returns the size of the local CSV file.
func (s *localCSVSource) Size(ctx context.Context) int64 {
	return s.size
}

// Next reads the next batch of CSV rows from the local file.
// It reads from the file at the specified offset and returns a batch of CSVRow.
func (s *localCSVSource) Next(ctx context.Context, offset any, size uint) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	offsetStr, ok := offset.(string)
	if !ok {
		return nil, ErrInvalidOffset
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return nil, ErrNilTransformer
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offsetStr,
		StartOffset: offsetStr,
		Done:        false,
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		l.Error("error opening local CSV file", "path", s.path, "error", err.Error())
		return nil, ErrLocalCSVFileOpen
	}
	defer f.Close()

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return bp, err
	}

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, offsetInt64)
	if err != nil && err != io.EOF {
		l.Error("error reading local CSV file", "path", s.path, "offset", offset, "error", err.Error())
		return nil, fmt.Errorf("error reading file %s at offset %s: %w", s.path, offset, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
	}

	records, nextOffset, err := ReadCSVBatch(
		ctx,
		data,
		numBytesRead,
		offsetInt64,
		s.delimiter,
		s.hasHeader,
		s.transFunc,
	)
	if err != nil {
		l.Error("error reading CSV data", "path", s.path, "offset", offset, "error", err.Error())

		bp.Records = records
		bp.NextOffset = utils.Int64ToString(nextOffset)
		bp.Done = done

		return bp, err
	}

	bp.Records = records
	bp.NextOffset = utils.Int64ToString(nextOffset)
	bp.Done = done

	return bp, nil
}

func (s *localCSVSource) NextStream(
	ctx context.Context,
	offset any,
	size uint,
) (<-chan *domain.BatchRecord, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	offsetStr, ok := offset.(string)
	if !ok {
		return nil, ErrInvalidOffset
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		l.Error("error opening local CSV file", "path", s.path, "error", err.Error())
		return nil, fmt.Errorf("%w: %v", ErrLocalCSVFileOpen, err)
	}
	defer f.Close()

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return nil, err
	}

	// Set start index to the specified offset.
	startIndex := offsetInt64

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		l.Error("error reading file", "path", s.path, "offset", startIndex, "error", err.Error())
		return nil, fmt.Errorf("error reading file %s at offset %d: %w", s.path, startIndex, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
	}

	resStream := make(chan *domain.BatchRecord)

	if done {
		resStream <- &domain.BatchRecord{
			Start: offset,
			End:   offset,
			Done:  done,
		}
	}

	go func() {
		defer close(resStream)
		err := ReadCSVStream(
			ctx,
			data,
			numBytesRead,
			offsetInt64,
			s.delimiter,
			s.hasHeader,
			s.transFunc,
			resStream,
		)
		if err != nil {
			l.Error("error reading CSV data stream", "path", s.path, "offset", offset, "error", err.Error())
		}
	}()

	return resStream, nil
}

// Local CSV source config.
type LocalCSVConfig struct {
	Path         string
	Delimiter    rune // e.g., ',', '|'
	HasHeader    bool
	MappingRules map[string]domain.Rule // mapping rules for the CSV rows
}

// Name of the source.
func (c *LocalCSVConfig) Name() string { return LocalCSVSource }

// BuildSource builds a local CSV source from the config.
// It reads headers if HasHeader is true and caches it.
func (c *LocalCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Path == "" {
		return nil, ErrLocalCSVPathRequired
	}

	delim := c.Delimiter
	if delim == 0 {
		delim = ',' // default
	}

	src := &localCSVSource{
		path:      c.Path,
		delimiter: delim,
		hasHeader: c.HasHeader,
	}

	if fileInfo, err := os.Stat(c.Path); err != nil {
		l.Error("error getting local CSV file info", "path", c.Path, "error", err.Error())
		return nil, ErrLocalCSVFileOpen
	} else {
		src.size = fileInfo.Size()
	}

	// If CSV file has headers, set cleaned headers.
	if c.HasHeader {
		f, err := os.Open(c.Path)
		if err != nil {
			l.Error("error opening local CSV file", "path", c.Path, "error", err.Error())
			return nil, ErrLocalCSVFileOpen
		}
		defer f.Close()

		if fileInfo, err := os.Stat(c.Path); err != nil {
			fmt.Printf("Error getting file info: %v\n", err)
		} else {
			src.size = fileInfo.Size()
		}

		r := csv.NewReader(f)
		r.Comma = delim
		r.FieldsPerRecord = -1

		h, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// empty file: treat as no headers
				h = nil
			} else {
				return nil, fmt.Errorf("local csv: read header: %w", err)
			}
		}
		headers := strutils.CleanHeaders(h)

		// build transformer function
		var rules map[string]domain.Rule
		if len(c.MappingRules) > 0 {
			rules = c.MappingRules
		} else {
			// If no mapping rules are provided, use default rules
			rules = domain.BuildBusinessModelTransformRules()
		}
		transFunc := domain.BuildTransformerWithRules(headers, rules)
		src.transFunc = transFunc
	}

	return src, nil
}
