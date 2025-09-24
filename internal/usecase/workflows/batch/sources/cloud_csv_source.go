package sources

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"

	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const CloudCSVSource = "cloud-csv-source"

const (
	ERR_CLOUD_CSV_READER_NIL           = "cloud csv: reader is nil"
	ERR_CLOUD_CSV_CLIENT_NIL           = "cloud csv: client is not initialized"
	ERR_CLOUD_CSV_FILE_SIZE_INVALID    = "cloud csv: size must be greater than 0"
	ERR_CLOUD_CSV_OBJECT_PATH_REQUIRED = "cloud csv: object path is required"
	ERR_CLOUD_CSV_BUCKET_REQUIRED      = "cloud csv: bucket name is required"
	ERR_CLOUD_CSV_UNSUPPORTED_PROVIDER = "cloud csv: unsupported provider, only 'gcs' is supported"
	ERR_CLOUD_CSV_MISSING_CREDENTIALS  = "cloud csv: missing credentials path"
	ERR_CLOUD_CSV_SIZE_INVALID         = "cloud csv: size must be greater than 0"
	ERR_CLOUD_INVALID_OFFSET           = "cloud csv: invalid offset, must be string"
)

var (
	ErrCloudCSVReaderNil           = errors.New(ERR_CLOUD_CSV_READER_NIL)
	ErrCloudCSVClientNil           = errors.New(ERR_CLOUD_CSV_CLIENT_NIL)
	ErrCloudCSVSizeMustBePositive  = errors.New(ERR_CLOUD_CSV_FILE_SIZE_INVALID)
	ErrCloudCSVObjectPathRequired  = errors.New(ERR_CLOUD_CSV_OBJECT_PATH_REQUIRED)
	ErrCloudCSVBucketRequired      = errors.New(ERR_CLOUD_CSV_BUCKET_REQUIRED)
	ErrCloudCSVUnsupportedProvider = errors.New(ERR_CLOUD_CSV_UNSUPPORTED_PROVIDER)
	ErrCloudCSVMissingCredentials  = errors.New(ERR_CLOUD_CSV_MISSING_CREDENTIALS)
	ErrCloudCSVSizeInvalid         = errors.New(ERR_CLOUD_CSV_SIZE_INVALID)
	ErrCloudInvalidOffset          = errors.New(ERR_CLOUD_INVALID_OFFSET)
)

type CloudSource string

const (
	CloudSourceGCS   CloudSource = "gcs"
	CloudSourceS3    CloudSource = "s3"
	CloudSourceAzure CloudSource = "azure"
)

// GCPStorageReadAtAdapter is an adapter for GCP Storage reader to implement ReadAt interface.
type GCPStorageReadAtAdapter struct {
	Reader *storage.Reader // storage.Reader is a GCP Storage reader
}

// ReadAt reads data from the GCP Storage reader at the specified offset.
func (g *GCPStorageReadAtAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	if g.Reader == nil {
		return 0, ErrCloudCSVReaderNil
	}

	// seek to the specified offset
	_, err = io.CopyN(io.Discard, g.Reader, off)
	if err != nil {
		return 0, err
	}

	return g.Reader.Read(p)
}

// Cloud CSV (S3/GCS/Azure) source.
type cloudCSVSource struct {
	provider  string // e.g., "s3", "gcs"
	path      string
	bucket    string
	size      int64
	delimiter rune
	hasHeader bool
	transFunc domain.TransformerFunc // transformer function to apply to each row
	client    *storage.Client        // GCP Storage client, if needed // GCP Storage client, if using GCS
}

func (s *cloudCSVSource) Close(ctx context.Context) error {
	return s.client.Close()
}

// Name of the source.
func (s *cloudCSVSource) Name() string { return CloudCSVSource }

// Size returns the size of the cloud CSV file.
func (s *cloudCSVSource) Size(ctx context.Context) int64 {
	return s.size
}

// Next reads the next batch of CSV rows from the cloud storage (S3/GCS/Azure).
// It reads from the cloud storage at the specified offset and returns a batch of CSVRow.
// Currently only supports GCP Storage. Ensure the environment variable is set for GCP credentials
func (s *cloudCSVSource) Next(
	ctx context.Context,
	offset any,
	size uint,
) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	offsetStr, ok := offset.(string)
	if !ok {
		return nil, ErrCloudInvalidOffset
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	// Ensure client is initialized
	if s.client == nil {
		return nil, ErrCloudCSVClientNil
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			l.Error("cloud csv: error closing reader", "error", err.Error())
		}
	}()

	// Set start index & done flag.
	// startIndex := int64(offset)
	done := false

	// Create a read-at adapter for the GCP Storage reader.
	// This allows us to read data at specific offsets.
	readAtAdapter := &GCPStorageReadAtAdapter{
		Reader: rc,
	}

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: invalid offset %s: %w", offset, err)
	}

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, offsetInt64)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading object %s in bucket %s at offset %s: %w", s.path, s.bucket, offset, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
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

func (s *cloudCSVSource) NextStream(
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
		return nil, ErrCloudInvalidOffset
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrCloudCSVSizeInvalid
	}

	// Ensure client is initialized
	if s.client == nil {
		return nil, ErrCloudCSVClientNil
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			l.Error("cloud csv: error closing reader", "error", err.Error())
		}
	}()

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: invalid offset %s: %w", offset, err)
	}

	// Set start index & done flag.
	startIndex := offsetInt64
	done := false

	// Create a read-at adapter for the GCP Storage reader.
	// This allows us to read data at specific offsets.
	readAtAdapter := &GCPStorageReadAtAdapter{
		Reader: rc,
	}

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading object %s in bucket %s at offset %d: %w", s.path, s.bucket, startIndex, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
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

// Cloud CSV (S3/GCS/Azure) - source config.
type CloudCSVConfig struct {
	Provider     string // "s3"|"gcs"|...
	Bucket       string
	Path         string
	Delimiter    rune // e.g., ',', '|'
	HasHeader    bool
	MappingRules map[string]domain.Rule
}

// Name of the source.
func (c *CloudCSVConfig) Name() string { return CloudCSVSource }

// BuildSource builds a cloud CSV source from the config.
func (c *CloudCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	// build s3/gcs/azure client from c.Provider, bucket, key
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Path == "" {
		return nil, ErrCloudCSVObjectPathRequired
	}

	if c.Bucket == "" {
		return nil, ErrCloudCSVBucketRequired
	}

	if c.Delimiter == 0 {
		c.Delimiter = ',' // default
	}

	if c.Provider == "" {
		c.Provider = "gcs" // default to GCS
	}

	if c.Provider != "gcs" {
		return nil, ErrCloudCSVUnsupportedProvider
	}

	// Ensure the environment variable is set for GCP credentials
	cPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if cPath == "" {
		return nil, ErrCloudCSVMissingCredentials
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: failed to create storage client: %w", err)
	}

	src := &cloudCSVSource{
		provider:  c.Provider,
		bucket:    c.Bucket,
		path:      c.Path,
		delimiter: c.Delimiter,
		hasHeader: c.HasHeader,
	}

	obj := client.Bucket(c.Bucket).Object(c.Path)
	if attrs, err := obj.Attrs(ctx); err != nil {
		//
		if err := client.Close(); err != nil {
			l.Error("cloud csv: error closing client", "error", err.Error())
		}
		return nil, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	} else {
		if attrs.Size <= 0 {
			if err := client.Close(); err != nil {
				log.Println("cloud csv: error closing client:", err)
			}
			return nil, ErrCloudCSVSizeMustBePositive
		}
		src.size = attrs.Size
	}

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", c.Path, c.Bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			l.Error("cloud csv: error closing reader", "error", err.Error())
		}
	}()

	if c.HasHeader {
		r := csv.NewReader(rc)
		r.Comma = c.Delimiter
		r.FieldsPerRecord = -1

		h, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// empty file: treat as no headers
				h = nil
			} else {
				return nil, fmt.Errorf("cloud csv: read header: %w", err)
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

	src.client = client

	return src, nil
}
