package env

import (
	"fmt"
	"os"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
	"github.com/hankgalt/workflow-scheduler/internal/domain/test"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
)

const DEFAULT_DATA_DIR = "data"
const DEFAULT_DATA_PATH string = "scheduler"
const DEFAULT_FILE_NAME string = "Agents-sm.csv"

// BuildLocalCSVBatchRequest constructs a LocalCSVBatchRequest with the specified max batches, batch size & relevant environment variables.
func BuildLocalCSVBatchRequest(max, size uint) (*batch.LocalCSVBatchRequest, error) {
	reqCfg, err := BuildLocalCSVBatchConfig()
	if err != nil {
		return nil, err
	}

	req := &batch.LocalCSVBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: reqCfg,
	}

	return req, nil
}

// BuildCloudCSVBatchRequest constructs a CloudCSVBatchRequest with the specified max batches, batch size & relevant environment variables.
func BuildCloudCSVBatchRequest(max, size uint) (*batch.CloudCSVBatchRequest, error) {
	reqCfg, err := BuildCloudCSVBatchConfig()
	if err != nil {
		return nil, err
	}

	req := &batch.CloudCSVBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: reqCfg,
	}

	return req, nil
}

// BuildLocalCSVMongoBatchRequest constructs a LocalCSVMongoBatchRequest with the specified max batches, batch size & relevant environment variables.
func BuildLocalCSVMongoBatchRequest(max, size uint) (*batch.LocalCSVMongoBatchRequest, error) {
	reqCfg, err := BuildLocalCSVMongoBatchConfig()
	if err != nil {
		return nil, err
	}

	req := &batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: reqCfg,
	}

	return req, nil
}

func BuildLocalCSVBatchConfig() (batch.LocalCSVBatchConfig, error) {
	filePath, err := BuildFilePath()
	if err != nil {
		return batch.LocalCSVBatchConfig{}, err
	}
	fileName := BuildFileName()

	return batch.LocalCSVBatchConfig{
		Name: fileName,
		Path: filePath,
	}, nil
}

func BuildCloudCSVBatchConfig() (batch.CloudCSVBatchConfig, error) {
	filePath, fileName := DEFAULT_DATA_PATH, BuildFileName()
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		return batch.CloudCSVBatchConfig{}, fmt.Errorf("BUCKET environment variable is not set")
	}

	return batch.CloudCSVBatchConfig{
		Name:   fileName,
		Path:   filePath,
		Bucket: bucket,
	}, nil
}

func BuildLocalCSVMongoBatchConfig() (batch.LocalCSVMongoBatchConfig, error) {
	filePath, err := BuildFilePath()
	if err != nil {
		return batch.LocalCSVMongoBatchConfig{}, err
	}
	fileName, mCfg := BuildFileName(), BuildMongoConfig()

	return batch.LocalCSVMongoBatchConfig{
		LocalCSVBatchConfig: batch.LocalCSVBatchConfig{
			Name: fileName,
			Path: filePath,
		},
		MongoBatchConfig: mCfg,
	}, nil
}

// BuildFilePath constructs the file path using the DATA_DIR env variable or defaults to "<DEFAULT_DATA_DIR>/<DEFAULT_DATA_PATH>".
func BuildFilePath() (string, error) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = DEFAULT_DATA_DIR
		fmt.Printf("DATA_DIR environment variable is not set, using default: %s\n", DEFAULT_DATA_DIR)
	}

	filePath := fmt.Sprintf("%s/%s", dataDir, DEFAULT_DATA_PATH)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("Data path does not exist: %s\n", filePath)
		return "", fmt.Errorf("data path does not exist: %s", filePath)
	}

	return filePath, nil
}

// BuildFileName constructs the file name using the FILE_NAME env variable or defaults to DEFAULT_FILE_NAME.
func BuildFileName() string {
	fileName := os.Getenv("FILE_NAME")
	if fileName == "" {
		fileName = DEFAULT_FILE_NAME
		fmt.Printf("FILE_NAME environment variable is not set, using default: %s\n", DEFAULT_FILE_NAME)
	}

	return fileName
}

func BuildMongoConfig() batch.MongoBatchConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbHost := os.Getenv("MONGO_HOSTNAME")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbParams := os.Getenv("MONGO_CONN_PARAMS")
	dbName := os.Getenv("MONGO_DBNAME")
	return batch.MongoBatchConfig{
		Protocol: dbProtocol,
		Host:     dbHost,
		User:     dbUser,
		Pwd:      dbPwd,
		Params:   dbParams,
		Name:     dbName,
	}
}

func BuildMongoStoreConfig() infra.StoreConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbHost := os.Getenv("MONGO_HOSTNAME")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbParams := os.Getenv("MONGO_CONN_PARAMS")
	dbName := os.Getenv("MONGO_DBNAME")
	return mongostore.NewMongoDBConfig(dbProtocol, dbHost, dbUser, dbPwd, dbParams, dbName)
}

func BuildTemporalConfig(clientName string) temporal.TemporalConfig {
	namespace := os.Getenv("WORKFLOW_DOMAIN")
	host := os.Getenv("TEMPORAL_HOST")
	metricsPort, otelEndpoint := BuildMetricsConfig()
	return temporal.NewTemporalConfig(namespace, host, clientName, metricsPort, otelEndpoint)
}

func BuildTestConfig() test.TestConfig {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = DEFAULT_DATA_DIR
	}
	bucket := os.Getenv("BUCKET")
	return test.NewTestConfig(dataDir, bucket)
}

func BuildMetricsConfig() (string, string) {
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9464"
	} else {
		metricsPort = fmt.Sprintf(":%s", metricsPort)
	}
	otelEndpoint := os.Getenv("OTEL_ENDPOINT")
	if otelEndpoint == "" {
		otelEndpoint = "otel-collector:4317"
	}
	return metricsPort, otelEndpoint
}
