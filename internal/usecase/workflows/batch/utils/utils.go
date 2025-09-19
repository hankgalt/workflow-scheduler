package utils

import (
	"fmt"
	"strings"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
)

func GenerateRunID(cfg any) (string, error) {
	switch v := cfg.(type) {
	case batch.LocalCSVBatchConfig:
		return fmt.Sprintf(
			"%s-%s",
			strings.ReplaceAll(v.Path, "/", "-"),
			v.Name,
		), nil
	case batch.CloudCSVBatchConfig:
		return fmt.Sprintf(
			"%s-%s-%s",
			v.Bucket,
			strings.ReplaceAll(v.Path, "/", "-"),
			v.Name,
		), nil
	case batch.LocalCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s-%s",
			strings.ReplaceAll(v.LocalCSVBatchConfig.Path, "/", "-"),
			v.LocalCSVBatchConfig.Name,
		), nil
	case batch.CloudCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s-%s-%s-%s",
			v.CloudCSVBatchConfig.Bucket,
			strings.ReplaceAll(v.CloudCSVBatchConfig.Path, "/", "-"),
			v.CloudCSVBatchConfig.Name,
			v.Collection,
		), nil
	default:
		return "", fmt.Errorf("unknown batch config type: %T", cfg)
	}
}
