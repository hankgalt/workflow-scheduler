package localcsvtomongo_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	localcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv"
	localcsvtomongo "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv_to_mongo"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	"github.com/hankgalt/workflow-scheduler/pkg/test"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

func TestLocalCSVToMongoHandler(t *testing.T) {
	t.Helper()

	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	testCfg := test.GetTestConfig()
	testFilePath := filepath.Join(testCfg.DataDir(), filePath)

	handlerCfg := localcsv.NewLocalCSVFileHandlerConfig(fileName, testFilePath)
	nmCfg := mongostore.GetMongoConfig()

	l := logger.GetSlogLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	hndlr, err := localcsvtomongo.NewLocalCSVToMongoHandler(ctx, handlerCfg, nmCfg)
	if err != nil {
		t.Logf("Error mongo handler: %v", err)
		return
	}

	batchSize := uint64(400)

	data, n, endOfFile, err := hndlr.ReadData(ctx, 0, batchSize)
	if err != nil {
		t.Logf("Error reading data: %v", err)
		return
	}

	headers := hndlr.Headers()
	require.NotEmpty(t, headers, "Headers should not be empty")

	require.Equal(t, true, n < batchSize, "Read less than batch size")

	t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)
	t.Logf("File headers: %v", hndlr.Headers())

	recStream, err := hndlr.HandleData(ctx, 0, data, headers)
	require.NoError(t, err)

	recordCnt, errorCnt, err := btchutils.ProcessCSVRecordStream(ctx, recStream)
	require.NoError(t, err)
	t.Logf("Processed %d records, with %d errors", recordCnt, errorCnt)

	offset, i := n, 1
	for !endOfFile {
		t.Logf("Reading next batch of data, offset: %d, batch size: %d", offset, batchSize)
		data, n, endOfFile, err = hndlr.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		t.Logf("Read data from file %s, with batch size: %d, next offset: %d", fileName, batchSize, n)

		recStream, err = hndlr.HandleData(ctx, 0, data, headers)
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
