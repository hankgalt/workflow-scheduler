package cloudcsv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	cloudcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
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

	fileHndlr, err := cloudcsv.NewCloudCSVFileHandler(handlerCfg)
	require.NoError(t, err)
	defer func() {
		err := fileHndlr.Close()
		require.NoError(t, err, "Error closing file handler")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	batchSize := uint64(1000)
	data, n, endOfFile, err := fileHndlr.ReadData(ctx, 0, batchSize)
	require.NoError(t, err)

	headers := fileHndlr.Headers()
	require.NotEmpty(t, headers, "Headers should not be empty")

	t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)
	t.Logf("File headers: %v", headers)

	recStream, err := fileHndlr.HandleData(ctx, 0, data, headers)
	require.NoError(t, err)

	recordCnt, errorCnt, err := btchutils.ProcessCSVRecordStream(ctx, recStream)
	require.NoError(t, err)
	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)

	offset, i := n, 1
	for !endOfFile {
		t.Logf("Reading next batch of data, offset: %d, batch size: %d", offset, batchSize)
		data, n, endOfFile, err = fileHndlr.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)

		recStream, err = fileHndlr.HandleData(ctx, 0, data, headers)
		require.NoError(t, err)

		recCnt, errCnt, err := btchutils.ProcessCSVRecordStream(ctx, recStream)
		require.NoError(t, err)
		t.Logf("Processed %d records in batch %d, with %d errors", recCnt, i+1, errCnt)

		recordCnt += recCnt
		errorCnt += errCnt

		offset = n
		i++
	}

	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)
}
