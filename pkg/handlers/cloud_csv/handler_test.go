package cloudcsv_test

import (
	"context"
	"testing"

	cloudcsv "github.com/hankgalt/workflow-scheduler/pkg/handlers/cloud_csv"
	"github.com/hankgalt/workflow-scheduler/pkg/test"
	"github.com/stretchr/testify/require"
)

func TestCloudCSVFileHandler(t *testing.T) {
	t.Helper()

	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	testCfg := test.GetTestConfig()

	handlerCfg := cloudcsv.NewCloudCSVFileHandlerConfig(fileName, filePath, testCfg.Bucket())
	fileHndlr, err := cloudcsv.NewCloudCSVFileHandler(handlerCfg)
	defer func() {
		err := fileHndlr.Close()
		require.NoError(t, err, "Error closing file handler")
	}()
	require.NoError(t, err)

	batchSize := uint64(1000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data, n, endOfFile, err := fileHndlr.ReadData(ctx, 0, batchSize)
	require.NoError(t, err)

	headers := fileHndlr.Headers()
	require.NotEmpty(t, headers, "Headers should not be empty")

	t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)
	t.Logf("File headers: %v", headers)

	recStream, err := fileHndlr.HandleData(ctx, 0, data, headers)
	require.NoError(t, err)

	recordCnt, errorCnt := test.ProcessCSVRecordStream(t, ctx, recStream)
	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)

	offset, i := n, 1
	for !endOfFile {
		t.Logf("Reading next batch of data, offset: %d, batch size: %d", offset, batchSize)
		data, n, endOfFile, err = fileHndlr.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)

		recStream, err = fileHndlr.HandleData(ctx, 0, data, headers)
		require.NoError(t, err)

		recCnt, errCnt := test.ProcessCSVRecordStream(t, ctx, recStream)
		t.Logf("Processed %d records in batch %d, with %d errors", recCnt, i+1, errCnt)

		recordCnt += recCnt
		errorCnt += errCnt

		offset += n
		i++
	}

	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)
}
