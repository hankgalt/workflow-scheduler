package sources_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
)

func Test_LocalJSONConfig_BuildSource(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ljsoncfg := sources.LocalJSONConfig{
		Path:    "./data/scheduler",
		FileKey: "dummy-job-multiple-key",
	}

	// Build the source
	source, err := ljsoncfg.BuildSource(ctx)
	require.NoError(t, err, "error building local json source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local json source")
	}()

	u := domain.JSONOffset{WithId: domain.WithId{Id: "0"}, Value: "0"}
	co := domain.CustomOffset[domain.HasId]{Val: u}

	bp, err := source.Next(ctx, co, 1)
	require.NoError(t, err, "error getting next batch from local json source")
	l.Info(
		"fetched first batch from local json source",
		"num-records", len(bp.Records),
		"start-offset", bp.StartOffset,
		"next-offset", bp.NextOffset,
		"done", bp.Done,
	)

	for !bp.Done {
		bp, err = source.Next(ctx, bp.NextOffset, 1)
		require.NoError(t, err, "error getting next batch from local json source")
		l.Debug(
			"fetched next batch from local json source",
			"num-records", len(bp.Records),
			"start-offset", bp.StartOffset,
			"next-offset", bp.NextOffset,
			"done", bp.Done,
		)
	}

	l.Info(
		"all batches processed from local json source, last batch",
		"num-records", len(bp.Records),
		"start-offset", bp.StartOffset,
		"next-offset", bp.NextOffset,
		"done", bp.Done,
	)
}

func Test_Live_JSONConfig_BuildSource(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ljsoncfg := sources.LocalJSONConfig{
		Path:    "../../../../../cmd/workers/batch/data/scheduler",
		FileKey: "local-csv-mongo-local-data-scheduler-Agents.csv",
	}

	// Build the source
	source, err := ljsoncfg.BuildSource(ctx)
	require.NoError(t, err, "error building local json source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local json source")
	}()

	u := domain.JSONOffset{WithId: domain.WithId{Id: "0"}, Value: "0"}
	co := domain.CustomOffset[domain.HasId]{Val: u}

	bp, err := source.Next(ctx, co, 1)
	require.NoError(t, err, "error getting next batch from local json source")
	l.Debug(
		"fetched batch from local json source",
		"num-records", len(bp.Records),
		"start-offset", bp.StartOffset,
		"next-offset", bp.NextOffset,
		"done", bp.Done,
	)
}
