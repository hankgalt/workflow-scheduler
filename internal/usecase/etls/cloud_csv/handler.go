package cloudcsv

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const (
	ERR_MISSING_CREDS_PATH = "missing credentials path for GCP Storage"
	ERR_MISSING_BUCKET     = "missing bucket name"
	ERR_MISSING_FILE_NAME  = "missing file name"
	ERR_FILE_ACCESS        = "error accessing storage file"
)

var (
	ErrMissingCredsPath  = errors.New(ERR_MISSING_CREDS_PATH)
	ErrMissingFileName   = errors.New(ERR_MISSING_FILE_NAME)
	ErrMissingBucketName = errors.New(ERR_MISSING_BUCKET)
	ErrFileAccess        = errors.New(ERR_FILE_ACCESS)
)

type GCPStorageReadAtAdapter struct {
	Reader *storage.Reader // storage.Reader is a GCP Storage reader
}

func (g *GCPStorageReadAtAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	if g.Reader == nil {
		return 0, ErrFileAccess
	}

	// seek to the specified offset
	_, err = io.CopyN(io.Discard, g.Reader, off)
	if err != nil {
		return 0, err
	}

	return g.Reader.Read(p)
}

type CloudCSVFileHandlerConfig struct {
	name, path string // Name & Path of the CSV file
	bucket     string // GCP Storage bucket name
}

func NewCloudCSVFileHandlerConfig(name, path, bucket string) CloudCSVFileHandlerConfig {
	return CloudCSVFileHandlerConfig{
		name:   name,
		path:   path,
		bucket: bucket,
	}
}

func (c CloudCSVFileHandlerConfig) Name() string   { return c.name }
func (c CloudCSVFileHandlerConfig) Path() string   { return c.path }
func (c CloudCSVFileHandlerConfig) Bucket() string { return c.bucket }

type CloudCSVFileHandler struct {
	client    *storage.Client
	dataPoint batch.CSVDataPoint
	headers   []string
}

// NewCloudCSVFileHandler creates a new CloudCSVFileHandler instance.
// It initializes the GCP Storage client and validates the configuration.
// The bucket name and file name must be provided in the configuration.
// It returns an error if the configuration is invalid or if the client cannot be created.
// The credentials path is expected to be set in the environment variable GOOGLE_APPLICATION_CREDENTIALS.
func NewCloudCSVFileHandler(cfg CloudCSVFileHandlerConfig) (*CloudCSVFileHandler, error) {
	if cfg.Name() == "" {
		return nil, ErrMissingFileName
	}

	if cfg.Bucket() == "" {
		return nil, ErrMissingBucketName
	}

	// Ensure the environment variable is set for GCP credentials
	cPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if cPath == "" {
		return nil, ErrMissingCredsPath
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &CloudCSVFileHandler{
		client:    client,
		dataPoint: batch.CSVDataPoint{Name: cfg.Name(), Path: cfg.Path(), Bucket: cfg.Bucket()},
	}, nil
}

func (h *CloudCSVFileHandler) Headers() []string {
	if h.headers == nil {
		return []string{} // Return empty slice if headers are not set
	}
	return h.headers
}

func (h *CloudCSVFileHandler) ReadData(ctx context.Context, offset, limit uint64) (any, uint64, bool, error) {
	if h.dataPoint.Name == "" {
		return nil, 0, false, fmt.Errorf("data point name is empty")
	}
	if h.dataPoint.Bucket == "" {
		return nil, 0, false, fmt.Errorf("data point bucket is empty")
	}

	fPath := h.dataPoint.Name
	if h.dataPoint.Path != "" {
		fPath = filepath.Join(h.dataPoint.Path, h.dataPoint.Name)
	}

	obj := h.client.Bucket(h.dataPoint.Bucket).Object(fPath)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, 0, false, fmt.Errorf("object %s does not exist in bucket %s", fPath, h.dataPoint.Bucket)
		}
		return nil, 0, false, fmt.Errorf("error getting object attributes: %w", err)
	}
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, 0, false, fmt.Errorf("error creating reader for object %s in bucket %s: %w", fPath, h.dataPoint.Bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			fmt.Printf("error closing reader for object %s in bucket %s: %v\n", fPath, h.dataPoint.Bucket, err)
		}
	}()

	rcReadAt := &GCPStorageReadAtAdapter{Reader: rc}

	data := make([]byte, limit)
	n, err := rcReadAt.ReadAt(data, int64(offset))
	if err != nil {
		return nil, 0, false, fmt.Errorf("error reading object %s at offset %d: %w", fPath, offset, err)
	}

	startOffset := 0
	if offset < 1 {
		buffer := bytes.NewBuffer(data)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1 // Read all fields

		headers, err := csvReader.Read()
		if err != nil {
			return nil, 0, false, fmt.Errorf("error reading CSV headers from object %s: %w", fPath, err)
		}
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

func (h *CloudCSVFileHandler) HandleData(
	ctx context.Context,
	start uint64,
	data any,
	headers []string,
) (<-chan batch.Result, error) {
	chnk, ok := data.([]byte)
	if !ok {
		return nil, errors.New("invalid data type, expected []byte")
	}

	recStream := make(chan batch.Result)
	go func() {
		defer close(recStream)

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
					fmt.Printf("  reached EOF, ending go routine. Last record - start: %d, end: %d\n", lastOffset, currOffset)
					return
				} else {
					cleanedStr := strutils.CleanRecord(string(chnk[currOffset:csvReader.InputOffset()]))
					record, err = strutils.ReadSingleRecord(cleanedStr)
					if err != nil {
						recStream <- batch.Result{
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
			recStream <- batch.Result{
				Start:  start + uint64(lastOffset),
				End:    start + uint64(currOffset),
				Record: cleanedRec,
			}
		}
	}()
	return recStream, nil
}

func (h *CloudCSVFileHandler) Close() error {
	if h.client != nil {
		return h.client.Close()
	}
	return nil
}
