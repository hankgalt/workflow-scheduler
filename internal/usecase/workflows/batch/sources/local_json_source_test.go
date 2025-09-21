package sources_test

import (
	"context"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	"github.com/stretchr/testify/require"
)

func Test_LocalJSONConfig_BuildSource(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ljsoncfg := sources.LocalJSONConfig{
		Path:    "../../../../../cmd/workers/batch/data/scheduler",
		FileKey: "local-csv-mongo-local-data-scheduler-Agents",
	}

	// Build the source
	source, err := ljsoncfg.BuildSource(ctx)
	require.NoError(t, err, "error building local json source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local json source")
	}()

	bp, err := source.Next(ctx, "0", 1)
	require.NoError(t, err, "error getting next batch from local json source")
	l.Debug("fetched batch from local json source", "batch", bp)
}
