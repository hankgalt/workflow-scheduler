package utils

import (
	"fmt"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
)

// func GenerateBatchID(cfg any, start, end uint64) (string, error) {
// 	switch v := cfg.(type) {
// 	case batch.LocalCSVBatchConfig:
// 		return fmt.Sprintf("%s/%s-%d-%d", v.Path, v.Name, start, end), nil
// 	case batch.CloudCSVBatchConfig:
// 		return fmt.Sprintf("%s-%s/%s-%d-%d", v.Bucket, v.Path, v.Name, start, end), nil
// 	case batch.LocalCSVMongoBatchConfig:
// 		return fmt.Sprintf(
// 			"%s/%s-%d-%d",
// 			v.LocalCSVBatchConfig.Path,
// 			v.LocalCSVBatchConfig.Name,
// 			start,
// 			end), nil
// 	case batch.CloudCSVMongoBatchConfig:
// 		return fmt.Sprintf(
// 			"%s-%s/%s-%s-%d-%d",
// 			v.CloudCSVBatchConfig.Bucket,
// 			v.CloudCSVBatchConfig.Path,
// 			v.CloudCSVBatchConfig.Name,
// 			v.Collection,
// 			start,
// 			end), nil
// 	default:
// 		return "", fmt.Errorf("unknown batch config type: %T", cfg)
// 	}
// }

func GenerateRunID(cfg any) (string, error) {
	switch v := cfg.(type) {
	case batch.LocalCSVBatchConfig:
		return fmt.Sprintf("%s/%s", v.Path, v.Name), nil
	case batch.CloudCSVBatchConfig:
		return fmt.Sprintf("%s-%s/%s", v.Bucket, v.Path, v.Name), nil
	case batch.LocalCSVMongoBatchConfig:
		return fmt.Sprintf("%s/%s", v.LocalCSVBatchConfig.Path, v.LocalCSVBatchConfig.Name), nil
	case batch.CloudCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s-%s/%s-%s",
			v.CloudCSVBatchConfig.Bucket,
			v.CloudCSVBatchConfig.Path,
			v.CloudCSVBatchConfig.Name,
			v.Collection), nil
	default:
		return "", fmt.Errorf("unknown batch config type: %T", cfg)
	}
}

// func ProcessCSVRecordStream(ctx context.Context, recStream <-chan batch.Result) (int, int, error) {
// 	l, err := logger.LoggerFromContext(ctx)
// 	if err != nil {
// 		return 0, 0, fmt.Errorf("ProcessCSVRecordStream - error getting logger from context: %w", err)
// 	}

// 	var processedCount, errorCount int

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			l.Info("Context cancelled, stopping processing CSV batch record stream")
// 			return processedCount, errorCount, ctx.Err()
// 		case rec, ok := <-recStream:
// 			if !ok {
// 				l.Info("Record stream closed")
// 				return processedCount, errorCount, nil
// 			}
// 			if rec.Error != "" {
// 				l.Debug("Error processing record", "error", rec.Error)
// 				errorCount++
// 			} else if rec.Record != nil {
// 				l.Debug("Processed record", "processedCount", processedCount+1, "record", rec.Record)
// 				processedCount++
// 			}
// 		}
// 	}
// }
