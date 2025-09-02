package batch

import (
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
)

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
	Collection          string                  `json:"collection"` // Collection name for MongoDB
}

type CloudCSVMongoBatchConfig struct {
	CloudCSVBatchConfig `json:"cloudCsvConfig"` // Configuration for cloud CSV batch processing
	MongoBatchConfig    `json:"mongoConfig"`    // Configuration for MongoDB batch processing
	Collection          string                  `json:"collection"` // Collection name for MongoDB
}

type RequestConfig struct {
	MaxBatches          uint                   `json:"maxBatches"`          // Maximum number of batches to process
	MaxInProcessBatches uint                   `json:"maxInProcessBatches"` // Maximum number of batches to process concurrently
	BatchSize           uint                   `json:"batchSize"`           // Size of each batch
	Start               uint64                 `json:"start"`               // Start position for processing
	MappingRules        map[string]domain.Rule `json:"mappingRules"`        // Optional mappings for CSV headers to MongoDB fields
}

type CSVBatchRequest struct {
	*RequestConfig `json:"requestConfig"` // Configuration for the batch request
}

type LocalCSVBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          LocalCSVBatchConfig      `json:"config"` // Configuration for the local CSV source
}

type CloudCSVBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          CloudCSVBatchConfig      `json:"config"` // Configuration for the cloud CSV source
}

type LocalCSVMongoBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          LocalCSVMongoBatchConfig `json:"config"` // Configuration for the local CSV and MongoDB source
}

type CloudCSVMongoBatchRequest struct {
	CSVBatchRequest `json:"csvBatchRequest"` // CSV batch request with additional fields
	Config          CloudCSVMongoBatchConfig `json:"config"` // Configuration for the cloud CSV and MongoDB source
}

type WorkflowQueryParams struct {
	RunId      string
	WorkflowId string
}

func MapRuleFromProto(protoRule *api.Rule) domain.Rule {
	return domain.Rule{
		Target:   protoRule.Target,
		Group:    protoRule.Group,
		NewField: protoRule.NewField,
		Order:    int(protoRule.Order),
	}
}

func MapRulesFromProto(protoRules map[string]*api.Rule) map[string]domain.Rule {
	rules := make(map[string]domain.Rule)
	for key, protoRule := range protoRules {
		rules[key] = MapRuleFromProto(protoRule)
	}
	return rules
}

func MapRuleToProto(rule domain.Rule) *api.Rule {
	return &api.Rule{
		Target:   rule.Target,
		Group:    rule.Group,
		NewField: rule.NewField,
		Order:    int32(rule.Order),
	}
}

func MapRulesToProto(rules []domain.Rule) []*api.Rule {
	var protoRules []*api.Rule
	for _, rule := range rules {
		protoRules = append(protoRules, MapRuleToProto(rule))
	}
	return protoRules
}
