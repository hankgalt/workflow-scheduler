package batch

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	cloudcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv"
	cloudcsvtomongo "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv_to_mongo"
	localcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv"
	localcsvtomongo "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv_to_mongo"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
)

const (
	SetupLocalCSVBatchActivity           = "SetupLocalCSVBatch"
	SetupCloudCSVBatchActivity           = "SetupCloudCSVBatch"
	SetupLocalCSVMongoBatchActivity      = "SetupLocalCSVMongoBatch"
	SetupCloudCSVMongoBatchActivity      = "SetupCloudCSVMongoBatch"
	HandleLocalCSVBatchDataActivity      = "HandleLocalCSVBatchData"
	HandleCloudCSVBatchDataActivity      = "HandleCloudCSVBatchData"
	HandleLocalCSVMongoBatchDataActivity = "HandleLocalCSVMongoBatchData"
	HandleCloudCSVMongoBatchDataActivity = "HandleCloudCSVMongoBatchData"
)

const (
	ERROR_INVALID_CONFIG_TYPE = "invalid configuration type for batch setup"
)

func SetupLocalCSVBatch(
	ctx context.Context,
	handlerCfg batch.LocalCSVBatchConfig,
	rqstCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	l := activity.GetLogger(ctx)

	l.Debug("SetupLocalCSVBatch - config", "name", handlerCfg.Name, "path", handlerCfg.Path)
	hndlr, err := getLocalCSVFileHandler(handlerCfg.Name, handlerCfg.Path)
	if err != nil {
		l.Error("SetupLocalCSVBatch - error creating handler", "error", err.Error())
		return rqstCfg, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return setupCSVBatch(ctx, hndlr, rqstCfg)
}

func SetupCloudCSVBatch(
	ctx context.Context,
	hndlrCfg batch.CloudCSVBatchConfig,
	rqstCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	l := activity.GetLogger(ctx)

	l.Debug(
		"SetupCloudCSVBatch - config",
		"name", hndlrCfg.Name,
		"path", hndlrCfg.Path,
		"bucket", hndlrCfg.Bucket,
	)

	handlerCfg := cloudcsv.NewCloudCSVFileHandlerConfig(
		hndlrCfg.Name,
		hndlrCfg.Path,
		hndlrCfg.Bucket,
	)
	hndlr, err := cloudcsv.NewCloudCSVFileHandler(handlerCfg)
	if err != nil {
		l.Error(
			"SetupCloudCSVBatch - error creating handler", "error",
			err.Error(),
		)
		return rqstCfg, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return setupCSVBatch(ctx, hndlr, rqstCfg)
}

func SetupLocalCSVMongoBatch(
	ctx context.Context,
	hndlrCfg batch.LocalCSVMongoBatchConfig,
	rqstCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	l := activity.GetLogger(ctx)

	l.Debug(
		"SetupLocalCSVMongoBatch - config",
		"name", hndlrCfg.LocalCSVBatchConfig.Name,
		"path", hndlrCfg.LocalCSVBatchConfig.Path,
		"mongoHost", hndlrCfg.MongoBatchConfig.Host,
	)

	csvCfg := localcsv.NewLocalCSVFileHandlerConfig(
		hndlrCfg.LocalCSVBatchConfig.Name,
		hndlrCfg.LocalCSVBatchConfig.Path,
	)
	mdbCfg := mongostore.NewMongoDBConfig(
		hndlrCfg.MongoBatchConfig.Protocol,
		hndlrCfg.MongoBatchConfig.Host,
		hndlrCfg.MongoBatchConfig.User,
		hndlrCfg.MongoBatchConfig.Pwd,
		hndlrCfg.MongoBatchConfig.Params,
		hndlrCfg.MongoBatchConfig.Name,
	)

	mCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mCtx = logger.WithLogger(mCtx, l)

	// Create a new LocalCSVToMongoHandler with the provided configurations
	hndlr, err := localcsvtomongo.NewLocalCSVToMongoHandler(mCtx, csvCfg, mdbCfg, hndlrCfg.Collection)
	if err != nil {
		l.Error("SetupLocalCSVMongoBatch - error creating handler", "error", err.Error())
		return rqstCfg, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return setupCSVBatch(ctx, hndlr, rqstCfg)
}

func SetupCloudCSVMongoBatch(
	ctx context.Context,
	hndlrCfg batch.CloudCSVMongoBatchConfig,
	rqstCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	l := activity.GetLogger(ctx)

	l.Debug(
		"SetupCloudCSVMongoBatch - config",
		"name", hndlrCfg.CloudCSVBatchConfig.Name,
		"path", hndlrCfg.CloudCSVBatchConfig.Path,
		"bucket", hndlrCfg.CloudCSVBatchConfig.Bucket,
		"mongoHost", hndlrCfg.MongoBatchConfig.Host,
	)

	csvCfg := cloudcsv.NewCloudCSVFileHandlerConfig(
		hndlrCfg.CloudCSVBatchConfig.Name,
		hndlrCfg.CloudCSVBatchConfig.Path,
		hndlrCfg.CloudCSVBatchConfig.Bucket,
	)
	mdbCfg := mongostore.NewMongoDBConfig(
		hndlrCfg.MongoBatchConfig.Protocol,
		hndlrCfg.MongoBatchConfig.Host,
		hndlrCfg.MongoBatchConfig.User,
		hndlrCfg.MongoBatchConfig.Pwd,
		hndlrCfg.MongoBatchConfig.Params,
		hndlrCfg.MongoBatchConfig.Name,
	)

	mCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mCtx = logger.WithLogger(mCtx, l)

	// Create a new CloudCSVToMongoHandler with the provided configurations
	hndlr, err := cloudcsvtomongo.NewCloudCSVToMongoHandler(mCtx, csvCfg, mdbCfg, hndlrCfg.Collection)
	if err != nil {
		l.Error("SetupCloudCSVMongoBatch - error creating handler", "error", err.Error())
		return rqstCfg, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return setupCSVBatch(ctx, hndlr, rqstCfg)
}

func setupCSVBatch(
	ctx context.Context,
	hndlr batch.CSVDataProcessor,
	rqstCfg *batch.RequestConfig,
) (*batch.RequestConfig, error) {
	l := activity.GetLogger(ctx)
	// Check if first batch
	// If first batch, initialize offsets & set the first offset to 0
	isFirstBatch := false
	if len(rqstCfg.Offsets) == 0 {
		isFirstBatch = true
		rqstCfg.Offsets = append(rqstCfg.Offsets, 0)
		activity.RecordHeartbeat(ctx, rqstCfg)
	}

	// Set batch start
	batchStart := rqstCfg.Offsets[len(rqstCfg.Offsets)-1]

	// Use batch start to get the next batch setup
	_, nextOffset, last, err := hndlr.ReadData(ctx, batchStart, uint64(rqstCfg.BatchSize))
	if err != nil {
		l.Error("setupCSVBatch - error reading data", "error", err.Error())
		return rqstCfg, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	l.Debug(
		"setupCSVBatch - data read",
		"batchStart", batchStart,
		"nextOffset", nextOffset,
		"endOfFile", last,
	)

	// If first batch, set the start offset & headers
	if isFirstBatch {
		if len(hndlr.Headers()) > 0 {
			l.Debug("setupCSVBatch - first batch fetched headers", "headers", hndlr.Headers())
			rqstCfg.Headers = hndlr.Headers()
			activity.RecordHeartbeat(ctx, rqstCfg)
		}
		// TODO - Update first offset from 0 to start of first record
		rqstCfg.Start = rqstCfg.Offsets[0] // Set start to starting offset
		activity.RecordHeartbeat(ctx, rqstCfg)
	}

	// Set batch end
	batchEnd := nextOffset

	// Append the next offset to the offsets list
	rqstCfg.Offsets = append(rqstCfg.Offsets, batchEnd)
	activity.RecordHeartbeat(ctx, rqstCfg)

	// If this is the last batch, set the end offset to the next offset
	// This is used to determine if the batch is complete
	if last {
		rqstCfg.End = nextOffset
		activity.RecordHeartbeat(ctx, rqstCfg)
	}

	l.Debug("setupCSVBatch - batch request state", "offsets", rqstCfg.Offsets)

	return rqstCfg, nil
}

func HandleLocalCSVBatchData(
	ctx context.Context,
	cfg batch.LocalCSVBatchConfig,
	rqstCfg *batch.RequestConfig,
	b *batch.Batch,
) (*batch.Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"HandleLocalCSVBatchData - config",
		"name", cfg.Name,
		"path", cfg.Path,
		"start", b.Start,
		"end", b.End,
	)

	// Create a handler for the local CSV file
	hndlr, err := getLocalCSVFileHandler(cfg.Name, cfg.Path)
	if err != nil {
		l.Error("HandleLocalCSVBatchData - error creating handler", "error", err.Error())
		return b, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return handleCSVBatchData(ctx, hndlr, rqstCfg, b)
}

func HandleCloudCSVBatchData(
	ctx context.Context,
	cfg batch.CloudCSVBatchConfig,
	rqstCfg *batch.RequestConfig,
	b *batch.Batch,
) (*batch.Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"HandleCloudCSVBatchData - config",
		"name", cfg.Name,
		"path", cfg.Path,
		"bucket", cfg.Bucket,
		"start", b.Start,
		"end", b.End)

	hndlr, err := getCloudCSVFileHandler(cfg.Name, cfg.Path, cfg.Bucket)
	if err != nil {
		l.Error("HandleCloudCSVBatchData - error creating handler", "error", err.Error())
		return b, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return handleCSVBatchData(ctx, hndlr, rqstCfg, b)
}

func HandleLocalCSVMongoBatchData(
	ctx context.Context,
	cfg batch.LocalCSVMongoBatchConfig,
	rqstCfg *batch.RequestConfig,
	b *batch.Batch,
) (*batch.Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"HandleLocalCSVMongoBatchData - config",
		"name", cfg.LocalCSVBatchConfig.Name,
		"path", cfg.LocalCSVBatchConfig.Path,
		"mongoHost", cfg.MongoBatchConfig.Host,
		"start", b.Start,
		"end", b.End)
	hndlr, err := getLocalCSVToMongoHandler(ctx, cfg)
	if err != nil {
		l.Error("HandleLocalCSVMongoBatchData - error creating handler", "error", err.Error())
		return b, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return handleCSVBatchData(ctx, hndlr, rqstCfg, b)
}

func HandleCloudCSVMongoBatchData(
	ctx context.Context,
	cfg batch.CloudCSVMongoBatchConfig,
	rqstCfg *batch.RequestConfig,
	b *batch.Batch,
) (*batch.Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"HandleCloudCSVMongoBatchData - config",
		"name", cfg.CloudCSVBatchConfig.Name,
		"path", cfg.CloudCSVBatchConfig.Path,
		"bucket", cfg.CloudCSVBatchConfig.Bucket,
		"mongoHost", cfg.MongoBatchConfig.Host,
		"start", b.Start,
		"end", b.End)
	hndlr, err := getCloudCSVToMongoHandler(ctx, cfg)
	if err != nil {
		l.Error("HandleCloudCSVMongoBatchData - error creating handler", "error", err.Error())
		return b, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return handleCSVBatchData(ctx, hndlr, rqstCfg, b)
}

func handleCSVBatchData(
	ctx context.Context,
	hndlr batch.CSVDataProcessor,
	rqstCfg *batch.RequestConfig,
	btch *batch.Batch,
) (*batch.Batch, error) {
	l := activity.GetLogger(ctx)
	// create context with logger for internal services
	hCtx := logger.WithLogger(ctx, l)

	// Use batch start to get the next batch setup
	data, _, _, err := hndlr.ReadData(hCtx, btch.Start, btch.End-btch.Start)
	if err != nil {
		l.Error("handleCSVBatchData - error reading data", "error", err.Error())
		return btch, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// Initialize records if nil
	if btch.Records == nil {
		btch.Records = map[string]*batch.Result{}
	}

	// get business entity transform rules & build the transformer function
	var rules map[string]batch.Rule
	if len(rqstCfg.MappingRules) > 0 {
		rules = rqstCfg.MappingRules
	} else {
		// If no mapping rules are provided, use default rules
		l.Debug("handleCSVBatchData - using default mapping rules")
		rules = btchutils.BuildBusinessModelTransformRules()
	}
	transFunc := btchutils.BuildTransformerWithRules(rqstCfg.Headers, rules)

	// Process data using the handler
	resStream, err := hndlr.HandleData(hCtx, btch.Start, data, transFunc)
	if err != nil {
		l.Error("handleCSVBatchData - error handling data", "error", err.Error())
		return btch, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	if activity.HasHeartbeatDetails(ctx) {
		// recover from finished progress
		var btchWithProgress *batch.Batch
		if err := activity.GetHeartbeatDetails(ctx, &btchWithProgress); err == nil {
			btch = btchWithProgress
		}
	}

	errCount, processedCount := 0, 0
	for {
		select {
		case <-ctx.Done():
			l.Debug("handleCSVBatchData - Context cancelled, stopping processing")
			return btch, temporal.NewApplicationErrorWithCause(ctx.Err().Error(), ctx.Err().Error(), ctx.Err())
		case res, ok := <-resStream:
			if !ok {
				l.Debug("handleCSVBatchData - Record stream closed")
				return btch, nil
			}

			// Generate a unique record ID based on the batch ID and record start/end positions
			recordId := fmt.Sprintf("%s-%d-%d", btch.BatchID, res.Start, res.End)

			// Skip processing if the record ID already exists in processed records.
			if btch.Records[recordId] != nil && btch.Records[recordId].Record != nil && btch.Records[recordId].Error == "" {
				l.Debug("handleCSVBatchData - record already processed", "recordId", recordId)
				processedCount++
				continue
			}

			if res.Error != "" {
				l.Debug("handleCSVBatchData - Error processing record", "result", res)
				res.RecordID = recordId // Set the record ID for the error result
				btch.Records[recordId] = &res
				activity.RecordHeartbeat(ctx, btch)
				errCount++
			} else if res.Record != nil {
				l.Debug("handleCSVBatchData - Processed record", "result", res)
				res.RecordID = recordId // Set the record ID for the processed result
				btch.Records[recordId] = &res
				activity.RecordHeartbeat(ctx, btch)
				processedCount++
			}
		}
	}
}

func getLocalCSVFileHandler(name, path string) (*localcsv.LocalCSVFileHandler, error) {
	handlerCfg := localcsv.NewLocalCSVFileHandlerConfig(name, path)
	hndlr, err := localcsv.NewLocalCSVFileHandler(handlerCfg)
	if err != nil {
		return nil, err
	}
	return hndlr, nil
}

func getCloudCSVFileHandler(name, path, bucket string) (*cloudcsv.CloudCSVFileHandler, error) {
	handlerCfg := cloudcsv.NewCloudCSVFileHandlerConfig(name, path, bucket)
	hndlr, err := cloudcsv.NewCloudCSVFileHandler(handlerCfg)
	if err != nil {
		return nil, err
	}
	return hndlr, nil
}

func getLocalCSVToMongoHandler(
	ctx context.Context,
	cfg batch.LocalCSVMongoBatchConfig,
) (*localcsvtomongo.LocalCSVToMongoHandler, error) {
	l := activity.GetLogger(ctx)

	csvCfg := localcsv.NewLocalCSVFileHandlerConfig(
		cfg.LocalCSVBatchConfig.Name,
		cfg.LocalCSVBatchConfig.Path,
	)
	mdbCfg := mongostore.NewMongoDBConfig(
		cfg.MongoBatchConfig.Protocol,
		cfg.MongoBatchConfig.Host,
		cfg.MongoBatchConfig.User,
		cfg.MongoBatchConfig.Pwd,
		cfg.MongoBatchConfig.Params,
		cfg.MongoBatchConfig.Name,
	)

	mCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mCtx = logger.WithLogger(mCtx, l)

	// Create a new LocalCSVToMongoHandler with the provided configurations
	hndlr, err := localcsvtomongo.NewLocalCSVToMongoHandler(mCtx, csvCfg, mdbCfg, cfg.Collection)
	if err != nil {
		return nil, err
	}
	return hndlr, nil
}

func getCloudCSVToMongoHandler(
	ctx context.Context,
	cfg batch.CloudCSVMongoBatchConfig,
) (*cloudcsvtomongo.CloudCSVToMongoHandler, error) {
	l := activity.GetLogger(ctx)

	csvCfg := cloudcsv.NewCloudCSVFileHandlerConfig(
		cfg.CloudCSVBatchConfig.Name,
		cfg.CloudCSVBatchConfig.Path,
		cfg.CloudCSVBatchConfig.Bucket,
	)
	mdbCfg := mongostore.NewMongoDBConfig(
		cfg.MongoBatchConfig.Protocol,
		cfg.MongoBatchConfig.Host,
		cfg.MongoBatchConfig.User,
		cfg.MongoBatchConfig.Pwd,
		cfg.MongoBatchConfig.Params,
		cfg.MongoBatchConfig.Name,
	)

	mCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mCtx = logger.WithLogger(mCtx, l)

	hndlr, err := cloudcsvtomongo.NewCloudCSVToMongoHandler(mCtx, csvCfg, mdbCfg, cfg.Collection)
	if err != nil {
		l.Error("getCloudCSVToMongoHandler - error creating handler", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	return hndlr, nil
}
