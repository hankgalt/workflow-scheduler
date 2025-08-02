package batch

import "context"

type DataReader interface {
	// ReadData reads data from data reader source at the specified offset and limit.
	// It returns the data read, the next offset to read from,
	// a boolean indicating if end of data at source was reached,
	// and any error encountered during the read operation.
	ReadData(ctx context.Context, offset, limit uint64) (any, uint64, bool, error)
}

type DataHandler interface {
	// HandleData processes the data.
	// The start parameter indicates the relative starting position for this chunk of data.
	// It returns a channel for results and errors.
	HandleData(ctx context.Context, start uint64, data any) (<-chan Result, error)
}

type CSVDataHandler interface {
	// HandleData processes the data.
	// The start parameter indicates the relative starting position for this chunk of data.
	// It returns a channel for results and errors.
	HandleData(ctx context.Context, start uint64, data any, headers []string) (<-chan Result, error)
}

type CSVSource interface {
	Headers() []string // Get headers for the CSV source
}

type DataProcessor interface {
	DataReader
	DataHandler
}

type CSVDataProcessor interface {
	CSVSource
	DataReader
	CSVDataHandler
}

type DataPoint struct {
	Name   string `json:"name"`   // Name of the data point
	Path   string `json:"path"`   // Path to the data point
	Bucket string `json:"bucket"` // Bucket where the data point is stored
}

type LocalCSVBatchConfig struct {
	Name string `json:"name"` // Name of the CSV file
	Path string `json:"path"` // Path to the CSV file
}

type CloudCSVBatchConfig struct {
	Name   string `json:"name"`   // Name of the CSV file
	Path   string `json:"path"`   // Path to the CSV file
	Bucket string `json:"bucket"` // Bucket where the CSV file is stored
}

type MongoBatchConfig struct {
	Protocol string `json:"protocol"` // Protocol for MongoDB connection (e.g., "mongodb")
	Host     string `json:"host"`     // Host for MongoDB connection
	User     string `json:"user"`     // User for MongoDB connection
	Pwd      string `json:"pwd"`      // Password for MongoDB connection
	Params   string `json:"params"`   // Additional parameters for MongoDB connection
	Name     string `json:"name"`     // Database name for MongoDB connection
}

type LocalCSVMongoBatchConfig struct {
	LocalCSVBatchConfig `json:"localCsvConfig"` // Configuration for local CSV batch processing
	MongoBatchConfig    `json:"mongoConfig"`    // Configuration for MongoDB batch processing
}

type Result struct {
	RecordID string `json:"recordId"` // Unique identifier for the record
	Start    uint64 `json:"start"`    // Start and end positions of the record in the file
	End      uint64 `json:"end"`      // End position of the record in the file
	Record   any    `json:"record"`   // Result data
	Error    string `json:"error"`    // Error message if any error occurred during processing
}

type Batch struct {
	BatchID string             `json:"batchId"` // Unique identifier for the batch
	Start   uint64             `json:"start"`   // Start position of the batch in the file
	End     uint64             `json:"end"`     // End position of the batch in the
	Records map[string]*Result `json:"records"` // List of records in the batch
}

type RequestConfig struct {
	MaxBatches uint     `json:"maxBatches"` // Maximum number of batches to process
	BatchSize  uint     `json:"batchSize"`  // Size of each batch
	Start      uint64   `json:"start"`      // Start position for processing
	End        uint64   `json:"end"`        // End position for processing
	Offsets    []uint64 `json:"offsets"`    // List of offsets for batch processing
	Headers    []string `json:"headers"`    // Headers for the CSV file
}

type CSVBatchRequest struct {
	*RequestConfig `json:"requestConfig"` // Configuration for the batch request
	Batches        map[string]*Batch      `json:"batches"` // Map of batch IDs to Batch objects
}

type LocalCSVBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          LocalCSVBatchConfig      // Configuration for the local CSV source
}

type CloudCSVBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          CloudCSVBatchConfig      // Configuration for the cloud CSV source
}

type LocalCSVMongoBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          LocalCSVMongoBatchConfig // Configuration for the local CSV and MongoDB source
}
