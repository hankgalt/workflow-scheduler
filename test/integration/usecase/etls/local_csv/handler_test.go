package localcsv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	localcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestLocalCSVFileHandler(t *testing.T) {
	t.Helper()

	testCfg := envutils.BuildTestConfig()
	require.NotEmpty(t, testCfg.DataDir(), "Data directory should not be empty")

	fileName := envutils.BuildFileName()
	require.NotEmpty(t, fileName, "File name should not be empty")

	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err)
	require.NotEmpty(t, filePath, "File path should not be empty")

	handlerCfg := localcsv.NewLocalCSVFileHandlerConfig(fileName, filePath)
	fileHndlr, err := localcsv.NewLocalCSVFileHandler(handlerCfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	batchSize := uint64(400)
	data, n, endOfFile, err := fileHndlr.ReadData(ctx, 0, batchSize)
	require.NoError(t, err)
	require.Equal(t, true, n < batchSize, "Read less than batch size")
	t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)

	headers := fileHndlr.Headers()
	t.Logf("File headers: %v", headers)

	resStream, err := fileHndlr.HandleData(ctx, 0, data, headers)
	require.NoError(t, err)

	recordCnt, errorCnt, err := btchutils.ProcessCSVRecordStream(ctx, resStream)
	require.NoError(t, err)
	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)

	offset, i := n, 1
	for !endOfFile {
		t.Logf("Reading next batch of data, offset: %d, batch size: %d", offset, batchSize)
		data, n, endOfFile, err = fileHndlr.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)

		resStream, err = fileHndlr.HandleData(ctx, 0, data, headers)
		require.NoError(t, err)

		recCnt, errCnt, err := btchutils.ProcessCSVRecordStream(ctx, resStream)
		require.NoError(t, err)
		t.Logf("Processed %d records in batch %d, with %d errors", recCnt, i+1, errCnt)

		recordCnt += recCnt
		errorCnt += errCnt

		offset = n
		i++
	}

	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)
}
