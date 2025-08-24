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

	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

const CloudCSVSource = "cloud-csv-source"

const (
	ERR_CLOUD_CSV_READER_NIL           = "cloud csv: reader is nil"
	ERR_CLOUD_CSV_CLIENT_NIL           = "cloud csv: client is not initialized"
	ERR_CLOUD_CSV_OBJECT_PATH_REQUIRED = "cloud csv: object path is required"
	ERR_CLOUD_CSV_BUCKET_REQUIRED      = "cloud csv: bucket name is required"
	ERR_CLOUD_CSV_UNSUPPORTED_PROVIDER = "cloud csv: unsupported provider, only 'gcs' is supported"
	ERR_CLOUD_CSV_MISSING_CREDENTIALS  = "cloud csv: missing credentials path"
	ERR_CLOUD_CSV_SIZE_INVALID         = "cloud csv: size must be greater than 0"
)

var (
	ErrCloudCSVReaderNil           = errors.New(ERR_CLOUD_CSV_READER_NIL)
	ErrCloudCSVClientNil           = errors.New(ERR_CLOUD_CSV_CLIENT_NIL)
	ErrCloudCSVObjectPathRequired  = errors.New(ERR_CLOUD_CSV_OBJECT_PATH_REQUIRED)
	ErrCloudCSVBucketRequired      = errors.New(ERR_CLOUD_CSV_BUCKET_REQUIRED)
	ErrCloudCSVUnsupportedProvider = errors.New(ERR_CLOUD_CSV_UNSUPPORTED_PROVIDER)
	ErrCloudCSVMissingCredentials  = errors.New(ERR_CLOUD_CSV_MISSING_CREDENTIALS)
	ErrCloudCSVSizeInvalid         = errors.New(ERR_CLOUD_CSV_SIZE_INVALID)
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

// Next reads the next batch of CSV rows from the cloud storage (S3/GCS/Azure).
// It reads from the cloud storage at the specified offset and returns a batch of CSVRow.
// Currently only supports GCP Storage. Ensure the environment variable is set for GCP credentials
func (s *cloudCSVSource) Next(
	ctx context.Context,
	offset uint64,
	size uint,
) (*domain.BatchProcess[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeInvalid
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("CloudCSVSource:Next - error getting logger from context: %w", err)
	}

	bp := &domain.BatchProcess[domain.CSVRow]{
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
			log.Printf("cloud csv: error closing reader: %v", err)
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

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading object %s in bucket %s at offset %d: %w", s.path, s.bucket, offset, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
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

func (s *cloudCSVSource) NextStream(
	ctx context.Context,
	offset uint64,
	size uint,
) (<-chan *domain.BatchRecord[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrCloudCSVSizeInvalid
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("CloudCSVSource:NextStream - error getting logger from context: %w", err)
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
			log.Printf("cloud csv: error closing reader: %v", err)
		}
	}()

	// Set start index & done flag.
	startIndex := int64(offset)
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
func (c CloudCSVConfig) Name() string { return CloudCSVSource }

// BuildSource builds a cloud CSV source from the config.
func (c CloudCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	// build s3/gcs/azure client from c.Provider, bucket, key

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

	obj := client.Bucket(c.Bucket).Object(c.Path)
	if _, err = obj.Attrs(ctx); err != nil {
		//
		if err := client.Close(); err != nil {
			log.Printf("cloud csv: error closing client: %v", err)
		}
		return nil, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	}

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", c.Path, c.Bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("cloud csv: error closing reader: %v", err)
		}
	}()

	src := &cloudCSVSource{
		provider:  c.Provider,
		bucket:    c.Bucket,
		path:      c.Path,
		delimiter: c.Delimiter,
		hasHeader: c.HasHeader,
	}

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
