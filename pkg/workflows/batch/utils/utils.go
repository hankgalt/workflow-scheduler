package utils

import (
	"fmt"

	"github.com/hankgalt/workflow-scheduler/pkg/models/batch"
)

func GenerateBatchID(cfg any, start, end uint64) (string, error) {
	switch v := cfg.(type) {
	case batch.LocalCSVBatchConfig:
		return fmt.Sprintf("%s/%s-%d-%d", v.Path, v.Name, start, end), nil
	case batch.CloudCSVBatchConfig:
		return fmt.Sprintf("%s-%s/%s-%d-%d", v.Bucket, v.Path, v.Name, start, end), nil
	case batch.LocalCSVMongoBatchConfig:
		return fmt.Sprintf("%s/%s-%d-%d", v.LocalCSVBatchConfig.Path, v.LocalCSVBatchConfig.Name, start, end), nil
	default:
		return "", fmt.Errorf("unknown batch config type: %T", cfg)
	}
}
