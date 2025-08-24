package sources_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func Test_CloudCSVConfig_BuildSource_FetxhNextBatch(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")
	path := filepath.Join(envCfg.Path, envCfg.Name)
	ccsvcfg := sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Build the source
	source, err := ccsvcfg.BuildSource(ctx)
	require.NoError(t, err, "error building local csv source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local csv source")
	}()

	bp, err := source.Next(ctx, 0, 400)
	require.NoError(t, err, "error getting next batch from local csv source")
	require.True(t, len(bp.Records) > 0, "no records found in the batch")

	for bp.Done == false {
		bp, err = source.Next(ctx, bp.NextOffset, 400)
		require.NoError(t, err, "error getting next batch from local csv source")
		require.True(t, len(bp.Records) > 0, "no records found in the batch")
	}

	require.True(t, bp.Done, "expected Done to be true at the end of file")
}
