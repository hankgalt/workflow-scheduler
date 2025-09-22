package env

import (
	"fmt"
	"os"

	"github.com/hankgalt/batch-orchestra/pkg/domain"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
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
			},
		},
		Config: reqCfg,
	}

	return req, nil
}

// BuildLocalCSVMongoBatchRequest constructs a LocalCSVMongoBatchRequest with the specified max batches, batch size & relevant environment variables.
func BuildLocalCSVMongoBatchRequest(max, size uint, mappingRules map[string]domain.Rule, direct bool) (*batch.LocalCSVMongoBatchRequest, error) {
	reqCfg, err := BuildLocalCSVMongoBatchConfig(direct)
	if err != nil {
		return nil, err
	}

	req := &batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches:   max,
				BatchSize:    size,
				MappingRules: mappingRules,
			},
		},
		Config: reqCfg,
	}

	return req, nil
}

// BuildCloudCSVMongoBatchRequest constructs a CloudCSVMongoBatchRequest with the specified max batches, batch size & relevant environment variables.
func BuildCloudCSVMongoBatchRequest(max, size uint, mappingRules map[string]domain.Rule, direct bool) (*batch.CloudCSVMongoBatchRequest, error) {
	reqCfg, err := BuildCloudCSVMongoBatchConfig(direct)
	if err != nil {
		return nil, err
	}

	req := &batch.CloudCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches:   max,
				BatchSize:    size,
				MappingRules: mappingRules,
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

func BuildLocalCSVMongoBatchConfig(direct bool) (batch.LocalCSVMongoBatchConfig, error) {
	filePath, err := BuildFilePath()
	if err != nil {
		return batch.LocalCSVMongoBatchConfig{}, err
	}
	fileName, mCfg := BuildFileName(), BuildMongoConfig(direct)
	collection, err := BuildMongoCollection()
	if err != nil {
		return batch.LocalCSVMongoBatchConfig{}, err
	}

	// fileName, mCfg := "Principals-sample.csv", BuildMongoConfig()
	// fileName, mCfg := "Filings-sample.csv", BuildMongoConfig()
	// collection := "vypar.filings"

	return batch.LocalCSVMongoBatchConfig{
		LocalCSVBatchConfig: batch.LocalCSVBatchConfig{
			Name: fileName,
			Path: filePath,
		},
		MongoBatchConfig: mCfg,
		Collection:       collection,
	}, nil
}

func BuildCloudCSVMongoBatchConfig(direct bool) (batch.CloudCSVMongoBatchConfig, error) {
	filePath, fileName := DEFAULT_DATA_PATH, BuildFileName()
	mCfg := BuildMongoConfig(direct)

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		return batch.CloudCSVMongoBatchConfig{}, fmt.Errorf("BUCKET environment variable is not set")
	}

	collection, err := BuildMongoCollection()
	if err != nil {
		return batch.CloudCSVMongoBatchConfig{}, err
	}

	return batch.CloudCSVMongoBatchConfig{
		CloudCSVBatchConfig: batch.CloudCSVBatchConfig{
			Name:   fileName,
			Path:   filePath,
			Bucket: bucket,
		},
		MongoBatchConfig: mCfg,
		Collection:       collection,
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

func BuildMongoCollection() (string, error) {
	collection := os.Getenv("MONGO_COLLECTION")
	if collection == "" {
		return "", fmt.Errorf("MONGO_COLLECTION environment variable is not set")
	}

	return collection, nil
}

func BuildMongoConfig(direct bool) batch.MongoBatchConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbName := os.Getenv("MONGO_DBNAME")

	dbHost := os.Getenv("MONGO_HOST_LIST")
	dbParams := os.Getenv("MONGO_CLUS_CONN_PARAMS")
	if direct {
		dbParams = os.Getenv("MONGO_DIR_CONN_PARAMS")
		dbHost = os.Getenv("MONGO_HOST_NAME")
	}
	return batch.MongoBatchConfig{
		Protocol: dbProtocol,
		Host:     dbHost,
		User:     dbUser,
		Pwd:      dbPwd,
		Params:   dbParams,
		Name:     dbName,
	}
}

func BuildMongoStoreConfig(direct bool) infra.StoreConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbName := os.Getenv("MONGO_DBNAME")

	dbHost := os.Getenv("MONGO_HOST_LIST")
	dbParams := os.Getenv("MONGO_CLUS_CONN_PARAMS")
	if direct {
		dbParams = os.Getenv("MONGO_DIR_CONN_PARAMS")
		dbHost = os.Getenv("MONGO_HOST_NAME")
	}
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

func BuildBusinessModelTransformRules() map[string]*api.Rule {
	return map[string]*api.Rule{
		"ENTITY_NUM":                  {Target: "ENTITY_ID"},                                // rename to ENTITY_ID
		"FIRST_NAME":                  {Target: "NAME", Group: true, Order: 1},              // group into NAME
		"MIDDLE_NAME":                 {Target: "NAME", Group: true, Order: 2},              // group into NAME
		"LAST_NAME":                   {Target: "NAME", Group: true, Order: 3},              // group into NAME
		"PHYSICAL_ADDRESS":            {Target: "ADDRESS"},                                  // rename to ADDRESS
		"PHYSICAL_ADDRESS1":           {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"PHYSICAL_ADDRESS2":           {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"PHYSICAL_ADDRESS3":           {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"PHYSICAL_CITY":               {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"PHYSICAL_STATE":              {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"PHYSICAL_POSTAL_CODE":        {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"PHYSICAL_COUNTRY":            {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"ADDRESS1":                    {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"ADDRESS2":                    {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"ADDRESS3":                    {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"CITY":                        {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"STATE":                       {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"POSTAL_CODE":                 {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"COUNTRY":                     {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"PRINCIPAL_ADDRESS":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 1}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS1":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 2}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS2":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 3}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_CITY":              {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 4}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_STATE":             {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 5}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_POSTAL_CODE":       {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 6}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_COUNTRY":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 7}, // group into PRINCIPAL_ADDRESS
		"MAILING_ADDRESS":             {Target: "MAILING_ADDRESS", Group: true, Order: 1},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS1":            {Target: "MAILING_ADDRESS", Group: true, Order: 2},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS2":            {Target: "MAILING_ADDRESS", Group: true, Order: 3},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS3":            {Target: "MAILING_ADDRESS", Group: true, Order: 4},   // group into MAILING_ADDRESS
		"MAILING_CITY":                {Target: "MAILING_ADDRESS", Group: true, Order: 5},   // group into MAILING_ADDRESS
		"MAILING_STATE":               {Target: "MAILING_ADDRESS", Group: true, Order: 6},   // group into MAILING_ADDRESS
		"MAILING_POSTAL_CODE":         {Target: "MAILING_ADDRESS", Group: true, Order: 7},   // group into MAILING_ADDRESS
		"MAILING_COUNTRY":             {Target: "MAILING_ADDRESS", Group: true, Order: 8},   // group into MAILING_ADDRESS
		"PRINCIPAL_ADDRESS_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 1},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS1_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 2},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS2_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 3},     // group into ADDRESS_IN_CA
		"PRINCIPAL_CITY_IN_CA":        {Target: "ADDRESS_IN_CA", Group: true, Order: 4},     // group into ADDRESS_IN_CA
		"PRINCIPAL_STATE_IN_CA":       {Target: "ADDRESS_IN_CA", Group: true, Order: 5},     // group into ADDRESS_IN_CA
		"PRINCIPAL_POSTAL_CODE_IN_CA": {Target: "ADDRESS_IN_CA", Group: true, Order: 6},     // group into ADDRESS_IN_CA
		"PRINCIPAL_COUNTRY_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 7},     // group into ADDRESS_IN_CA
		"POSITION_TYPE":               {Target: "AGENT_TYPE", NewField: "Principal"},        // new Target field AGENT_TYPE with value Principal
	}
}
