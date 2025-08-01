package localcsv_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	localcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	"github.com/hankgalt/workflow-scheduler/pkg/test"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

func TestLocalCSVFileHandler(t *testing.T) {
	t.Helper()

	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	testCfg := test.GetTestConfig()
	testFilePath := filepath.Join(testCfg.DataDir(), filePath)

	handlerCfg := localcsv.NewLocalCSVFileHandlerConfig(fileName, testFilePath)
	fileHndlr, err := localcsv.NewLocalCSVFileHandler(handlerCfg)
	require.NoError(t, err)

	batchSize := uint64(400)

	l := logger.GetSlogLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	data, n, endOfFile, err := fileHndlr.ReadData(ctx, 0, batchSize)
	if err != nil {
		t.Logf("Error reading data: %v", err)
		return
	}
	require.Equal(t, true, n < batchSize, "Read less than batch size")

	headers := fileHndlr.Headers()

	t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)
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
