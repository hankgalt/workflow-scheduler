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

	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const LocalCSVSource = "local-csv-source"

const (
	ERR_LOCAL_CSV_PATH_REQUIRED   = "local csv: path is required"
	ERR_LOCAL_CSV_TRANSFORMER_NIL = "local csv: transformer function is not set for local CSV source with headers"
	ERR_LOCAL_CSV_FILE_OPEN       = "local csv: open"
	ERR_LOCAL_CSV_SIZE_INVALID    = "local csv: size must be greater than 0"
)

var (
	ErrLocalCSVPathRequired = errors.New(ERR_LOCAL_CSV_PATH_REQUIRED)
	ErrLocalCSVFileOpen     = errors.New(ERR_LOCAL_CSV_FILE_OPEN)
	ErrLocalCSVSizeInvalid  = errors.New(ERR_LOCAL_CSV_SIZE_INVALID)
)

// Local CSV source.
type localCSVSource struct {
	path      string
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

// Next reads the next batch of CSV rows from the local file.
// It reads from the file at the specified offset and returns a batch of CSVRow.
func (s *localCSVSource) Next(ctx context.Context, offset uint64, size uint) (*domain.BatchProcess[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("localCSVSource:Next - error getting logger from context: %w", err)
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return nil, fmt.Errorf("transformer function is not set for local CSV source with headers")
	}

	bp := &domain.BatchProcess[domain.CSVRow]{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		l.Error("error opening local CSV file", "path", s.path, "error", err.Error())
		return nil, fmt.Errorf("%w: %v", ErrLocalCSVFileOpen, err)
	}
	defer f.Close()

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		l.Error("error reading local CSV file", "path", s.path, "offset", offset, "error", err.Error())
		return nil, fmt.Errorf("error reading file %s at offset %d: %w", s.path, offset, err)
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
		int64(offset),
		s.delimiter,
		s.hasHeader,
		s.transFunc,
	)
	if err != nil {
		l.Error("error reading CSV data", "path", s.path, "offset", offset, "error", err.Error())

		bp.Records = records
		bp.NextOffset = nextOffset
		bp.Done = done

		return bp, err
	}

	bp.Records = records
	bp.NextOffset = nextOffset
	bp.Done = done

	return bp, nil
}

func (s *localCSVSource) NextStream(
	ctx context.Context,
	offset uint64,
	size uint,
) (<-chan *domain.BatchRecord[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("localCSVSource:NextStream - error getting logger from context: %w", err)
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		l.Error("error opening local CSV file", "path", s.path, "error", err.Error())
		return nil, fmt.Errorf("%w: %v", ErrLocalCSVFileOpen, err)
	}
	defer f.Close()

	// Set start index to the specified offset.
	startIndex := int64(offset)

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

	resStream := make(chan *domain.BatchRecord[domain.CSVRow])

	if done {
		resStream <- &domain.BatchRecord[domain.CSVRow]{
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
			int64(offset),
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
func (c LocalCSVConfig) Name() string { return LocalCSVSource }

// BuildSource builds a local CSV source from the config.
// It reads headers if HasHeader is true and caches it.
func (c LocalCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
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

	// If CSV file has headers, set cleaned headers.
	if c.HasHeader {
		f, err := os.Open(c.Path)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrLocalCSVFileOpen, err)
		}
		defer f.Close()

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
