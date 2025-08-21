package cloudcsvtomongo_test

import (
	"context"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	cloudcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv"
	cloudcsvtomongo "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv_to_mongo"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestCloudCSVFileHandler(t *testing.T) {
	t.Helper()

	// Ensure the environment is set up correctly
	// requires GCP credentials path set in the environment
	testCfg := envutils.BuildTestConfig()
	require.NotEmpty(t, testCfg.Bucket(), "Bucket should not be empty")
	require.NotEmpty(t, testCfg.DataDir(), "Data directory should not be empty")

	fileName := envutils.BuildFileName()
	require.NotEmpty(t, fileName, "File name should not be empty")

	handlerCfg := cloudcsv.NewCloudCSVFileHandlerConfig(fileName, envutils.DEFAULT_DATA_PATH, testCfg.Bucket())

	collName, err := envutils.BuildMongoCollection()
	require.NoError(t, err, "Mongo collection name should be set")

	// create mongo store configuration
	nmCfg := envutils.BuildMongoStoreConfig()

	// get test logger
	l := logger.GetSlogLogger()

	// setup processing context with appropriate timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	hndlr, err := cloudcsvtomongo.NewCloudCSVToMongoHandler(ctx, handlerCfg, nmCfg, collName)
	require.NoError(t, err)

	// setup batch size
	batchSize := uint64(400)

	// read first batch data from local csv file
	data, n, endOfFile, err := hndlr.ReadData(ctx, 0, batchSize)
	require.NoError(t, err)

	// verify first batch has headers & only has complete records
	headers := hndlr.Headers()
	require.NotEmpty(t, headers, "Headers should not be empty")
	require.Equal(t, true, n < batchSize, "Read less than batch size")
	l.Debug(
		"TestCloudCSVFileHandler - Read data from file",
		"fileName", fileName,
		"batchSize", batchSize,
		"headers", headers,
		"nextOffset", n,
	)

	// get business model transform rules & build the transformer function
	rules := batch.BuildBusinessModelTransformRules()
	transFunc := batch.BuildTransformerWithRules(hndlr.Headers(), rules)

	// handle data read from local csv file & acquire record stream
	recStream, err := hndlr.HandleData(ctx, 0, data, transFunc)
	require.NoError(t, err)

	// process record stream for record count & error count
	recordCnt, errorCnt, err := btchutils.ProcessCSVRecordStream(ctx, recStream)
	require.NoError(t, err)
	l.Debug(
		"TestCloudCSVFileHandler - Processed records",
		"recordCount", recordCnt,
		"errorCount", errorCnt,
	)

	// iterate through the rest of the file until end of file
	offset, i := n, 1
	for !endOfFile {
		l.Debug(
			"TestCloudCSVFileHandler - Reading next batch of data",
			"offset", offset,
			"batchSize", batchSize,
		)
		data, n, endOfFile, err = hndlr.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		l.Debug(
			"TestCloudCSVFileHandler - Read data from file",
			"fileName", fileName,
			"batchSize", batchSize,
			"nextOffset", n,
		)

		recStream, err = hndlr.HandleData(ctx, 0, data, transFunc)
		require.NoError(t, err)

		recCnt, errCnt, err := btchutils.ProcessCSVRecordStream(ctx, recStream)
		require.NoError(t, err)
		l.Debug(
			"TestCloudCSVFileHandler - Processed records",
			"recordCount", recCnt,
			"batch", i+1,
			"errorCount", errCnt,
		)

		// update total record count & error count
		recordCnt += recCnt
		errorCnt += errCnt

		offset = n
		i++
	}

	l.Debug(
		"TestCloudCSVFileHandler - Processed records",
		"recordCount", recordCnt,
		"errorCount", errorCnt,
	)
}
