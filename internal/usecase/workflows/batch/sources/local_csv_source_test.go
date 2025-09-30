package sources_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"

	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func Test_LocalCSVConfig_BuildSource_FetxhNextBatch(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")

	path := filepath.Join(filePath, fileName)
	lcsvcfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Build the source
	source, err := lcsvcfg.BuildSource(ctx)
	require.NoError(t, err, "error building local csv source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local csv source")
	}()

	bp, err := source.Next(ctx, "0", 400)
	require.NoError(t, err, "error getting next batch from local csv source")
	require.True(t, len(bp.Records) > 0, "no records found in the batch")
	validateRecords(t, bp.Records)

	for bp.Done == false {
		bp, err = source.Next(ctx, bp.NextOffset, 400)
		require.NoError(t, err, "error getting next batch from local csv source")
		require.True(t, len(bp.Records) > 0, "no records found in the batch")
		validateRecords(t, bp.Records)
	}
	validateRecords(t, bp.Records)
	require.True(t, bp.Done, "expected Done to be true at the end of file")
}

func validateRecords(t *testing.T, records []*domain.BatchRecord) {
	for _, record := range records {
		validateRecord(t, record)
	}
}

func validateRecord(t *testing.T, record *domain.BatchRecord) {
	stStr, ok := record.Start.(string)
	require.True(t, ok, "expected start to be of type string")
	sOffset, err := utils.ParseInt64(stStr)
	require.NoError(t, err)

	etStr, ok := record.End.(string)
	require.True(t, ok, "expected end to be of type string")
	eOffset, err := utils.ParseInt64(etStr)
	require.NoError(t, err)

	require.True(t, eOffset > sOffset, "expected record End to be greater than Start")
}
