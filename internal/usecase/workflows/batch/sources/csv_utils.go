package sources

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const ERR_EMPTY_RECORD = "empty record"
const ERR_ALL_BATCH_RECORDS = "all batch records failed"

var ErrAllBatchRecords = errors.New(ERR_ALL_BATCH_RECORDS)

func ReadCSVBatch(
	ctx context.Context,
	data []byte,
	numBytesRead int,
	offset int64,
	delimiter rune,
	hasHeader bool,
	transFunc domain.TransformerFunc,
) ([]*domain.BatchRecord, int64, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if len(data) == 0 || numBytesRead == 0 {
		return nil, offset, nil
	}

	// Set start index to the specified offset.
	startIndex := offset

	// get last line break index to drop partial record
	i := bytes.LastIndex(data, []byte{'\n'})
	if i < 0 {
		// If no newline found, we read till the end, set nextOffset to numBytesRead
		i = numBytesRead - 1
	}

	// create data buffer for bytes upto last line break
	buffer := bytes.NewBuffer(data[:i+1])

	// Create a CSV reader with the buffer
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

	// Initialize start & next offsets
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0

	records := []*domain.BatchRecord{}

	var errCount int
	var errs map[string]int

	// Read records from the CSV reader
	for {
		// allow cancellation
		select {
		case <-ctx.Done():
			return records, offset + nextOffset, ctx.Err()
		default:
		}

		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Attempt record cleanup if error occurs
			cleanedStr := strutils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			if record, err := strutils.ReadSingleRecord(cleanedStr); err != nil {
				l.Error(
					"error reading record from csv",
					"start", nextOffset,
					"end", csvReader.InputOffset(),
					"error", err.Error(),
				)
				if errs == nil {
					errs = make(map[string]int)
				}
				errs[err.Error()]++
				errCount++

				records = append(records, &domain.BatchRecord{
					Start: utils.Int64ToString(nextOffset),
					End:   utils.Int64ToString(csvReader.InputOffset()),
					BatchResult: domain.BatchResult{
						Error: err.Error(),
					},
				})

				// Update startIndex to the next record's offset
				startIndex = nextOffset

				// update nextOffset to the next record's offset
				nextOffset = csvReader.InputOffset()
				// update row read count
				readCount++

				continue
			} else {
				rec = record
			}
		}

		// startIndex & nextOffset will be 0 for the first read, skip headers
		if startIndex <= 0 && nextOffset <= 0 && hasHeader {
			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			continue
		}

		// Clean record & create a CSVRow from the transformed record
		if len(rec) > 0 {
			rec = strutils.CleanAlphaNumericsArr(rec, []rune{'.', '-', '_', '#', '&', '@'})
		}

		if len(rec) == 0 {
			// Create a BatchRecord for the current csv record
			br := domain.BatchRecord{
				Start: utils.Int64ToString(startIndex),
				End:   utils.Int64ToString(csvReader.InputOffset()),
				BatchResult: domain.BatchResult{
					Error: ERR_EMPTY_RECORD,
				},
			}

			if errs == nil {
				errs = make(map[string]int)
			}
			errs[ERR_EMPTY_RECORD]++
			errCount++

			// Update records slice & read count
			records = append(records, &br)

			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			continue
		}

		res := transFunc(rec)
		row := domain.CSVRow{}
		for k, v := range res {
			st, ok := v.(string)
			if !ok {
				row[k] = ""
			}
			row[k] = st
		}

		// Create a BatchRecord for the current csv record
		br := domain.BatchRecord{
			Start: utils.Int64ToString(startIndex),
			End:   utils.Int64ToString(csvReader.InputOffset()),
			Data:  row,
		}

		// Update records slice & read count
		records = append(records, &br)

		// Update startIndex to the next record's offset
		startIndex = nextOffset

		// update nextOffset to the next record's offset
		nextOffset = csvReader.InputOffset()

		// update row read count
		readCount++

	}

	if errCount >= len(records) {
		l.Error("all batch records failed", "errors", errs)
		return records, offset + nextOffset, ErrAllBatchRecords
	}

	return records, offset + nextOffset, nil
}

func ReadCSVStream(
	ctx context.Context,
	data []byte,
	numBytesRead int,
	offset int64,
	delimiter rune,
	hasHeader bool,
	transFunc domain.TransformerFunc,
	resStream chan<- *domain.BatchRecord,
) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if len(data) == 0 || numBytesRead == 0 {
		return nil
	}

	// get last line break index to drop partial record
	i := bytes.LastIndex(data, []byte{'\n'})
	if i < 0 {
		// If no newline found, we read till the end, set nextOffset to numBytesRead
		i = numBytesRead - 1
	}

	// create data buffer for bytes upto last line break
	buffer := bytes.NewBuffer(data[:i+1])

	// Create a CSV reader with the buffer
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

	// Initialize start & next offsets
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0

	// Set start index to the specified offset.
	startIndex := offset

	// Read records from the CSV reader
	for {
		// allow cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Attempt record cleanup if error occurs
			cleanedStr := strutils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			record, err := strutils.ReadSingleRecord(cleanedStr)
			if err != nil {
				l.Error("error reading record from csv", "start", nextOffset, "end", csvReader.InputOffset(), "error", err.Error())
				resStream <- &domain.BatchRecord{
					Start: utils.Int64ToString(nextOffset),
					End:   utils.Int64ToString(csvReader.InputOffset()),
					BatchResult: domain.BatchResult{
						Error: fmt.Sprintf("read data row: %v", err),
					},
				}

				// Update startIndex to the next record's offset
				startIndex = nextOffset

				// update nextOffset to the next record's offset
				nextOffset = csvReader.InputOffset()
				// update row read count
				readCount++

				continue
			}

			rec = record
		}

		// startIndex & nextOffset will be 0 for the first read, skip headers
		if startIndex <= 0 && nextOffset <= 0 && hasHeader {
			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			continue
		}

		// Update startIndex to the next record's offset
		startIndex = nextOffset

		// update nextOffset to the next record's offset
		nextOffset = csvReader.InputOffset()

		// update row read count
		readCount++

		// Create a CSVRow from the transformed record
		rec = strutils.CleanAlphaNumericsArr(rec, []rune{'.', '-', '_', '#', '&', '@'})
		res := transFunc(rec)

		row := domain.CSVRow{}
		for k, v := range res {
			st, ok := v.(string)
			if !ok {
				row[k] = ""
			}
			row[k] = st
		}

		resStream <- &domain.BatchRecord{
			Start: utils.Int64ToString(startIndex),
			End:   utils.Int64ToString(csvReader.InputOffset()),
			Data:  row,
		}
	}
}
