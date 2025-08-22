package batch_test

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	bsinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

type ETLRequest[T any] struct {
	MaxBatches uint
	BatchSize  uint
	Done       bool
	Offsets    []uint64
	Batches    map[string]*domain.BatchProcess[T]
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func Test_FetchNext_LocalTempCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation to call.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)

	// Build a small CSV
	path := writeTempCSV(t,
		[]string{"id", "name"},
		[][]string{
			{"1", "alpha"},
			{"2", "beta"},
			{"3", "gamma"},
			{"4", "delta"},
			{"5", "epsilon"},
		},
	)
	defer func() {
		require.NoError(t, os.Remove(path), "cleanup temp CSV file")
	}()

	// Create a local CSV config
	cfg := sources.LocalCSVConfig{
		Path:      path,
		HasHeader: true,
	}

	// initialize batch size & next offset
	// Use a batch size larger than the largest row size in bytes to ensure more than one row is fetched.
	batchSize := uint(30)
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.False(t, out.Batch.Done)
	require.Len(t, out.Batch.Records, 2)
	require.EqualValues(t, 23, out.Batch.NextOffset)
	require.Equal(t, domain.CSVRow{"id": "1", "name": "alpha"}, out.Batch.Records[0].Data)
	require.Equal(t, domain.CSVRow{"id": "2", "name": "beta"}, out.Batch.Records[1].Data)

	// Fetch the next batch of records till done
	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
		require.NoError(t, err)
		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	}

	require.True(t, out.Batch.Done)
	require.EqualValues(t, 49, out.Batch.NextOffset)

	l.Debug("FetchNext_LocalTempCSV completed", "next-offset", out.Batch.NextOffset)
}

func Test_FetchNext_LocalCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation we will call.
	// In test env, this is optional for function activities, but explicit is fine.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)

	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building file path for test CSV")

	path := filepath.Join(filePath, fileName)

	cfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	l.Debug("Test_FetchNext_LocalCSV first batch completed", "next-offset", out.Batch.NextOffset)

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err = env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
		l.Debug("Test_FetchNext_LocalCSV batch completed", "next-offset", out.Batch.NextOffset)
	}
	l.Debug("Test_FetchNext_LocalCSV Done", "final-offset", out.Batch.NextOffset)
}

func Test_FetchNext_CloudCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register activity
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchAlias,
		},
	)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	cfg := sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	l.Debug("Test_FetchNext_CloudCSV first batch", "next-offset", out.Batch.NextOffset)

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
		l.Debug("Test_FetchNext_CloudCSV batch completed", "next-offset", out.Batch.NextOffset)
	}

	l.Debug("Test_FetchNext_CloudCSV Done", "final-offset", out.Batch.NextOffset)
}

func Test_Write_NoopSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.NoopSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextNoopSinkBatchAlias,
		},
	)

	recs := []*domain.BatchRecord[domain.CSVRow]{
		{
			Start: 0,
			End:   10,
			Data:  domain.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: 10,
			End:   20,
			Data:  domain.CSVRow{"id": "2", "name": "beta"},
		},
	}
	b := &domain.BatchProcess[domain.CSVRow]{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	in := &domain.WriteInput[domain.CSVRow, bsinks.NoopSinkConfig[domain.CSVRow]]{
		Sink:  bsinks.NoopSinkConfig[domain.CSVRow]{},
		Batch: b,
	}

	val, err := env.ExecuteActivity(btchwkfl.WriteNextNoopSinkBatchAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))
}

func Test_Write_MongoSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchAlias,
		},
	)

	recs := []*domain.BatchRecord[domain.CSVRow]{
		{
			Start: 0,
			End:   10,
			Data:  domain.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: 10,
			End:   20,
			Data:  domain.CSVRow{"id": "2", "name": "beta"},
		},
	}

	b := &domain.BatchProcess[domain.CSVRow]{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	nmCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, nmCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, nmCfg.Host(), "MongoDB host should not be empty")

	cfg := bsinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   nmCfg.Protocol(),
		Host:       nmCfg.Host(),
		DBName:     nmCfg.Name(),
		User:       nmCfg.User(),
		Pwd:        nmCfg.Pwd(),
		Params:     nmCfg.Params(),
		Collection: "test.people",
	}

	in := &domain.WriteInput[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  cfg,
		Batch: b,
	}

	val, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))
}

func Test_FetchAndWrite_LocalCSVSource_MongoSink_Queue(t *testing.T) {
	// setup test environment
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	// setup logger & global activity execution context
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := bsinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	// Build ETL request
	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*domain.BatchProcess[domain.CSVRow]{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := &domain.WriteInput[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId := fmt.Sprintf("batch-%d-%d", wOut.Batch.StartOffset, wOut.Batch.NextOffset)
			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

	require.Equal(t, true, len(etlReq.Offsets) > 0)
}

func Test_FetchAndWrite_CloudCSVSource_MongoSink_Queue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})
	env.SetTestTimeout(24 * time.Hour)

	// Register concrete generic instantiations used by the test.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchAlias,
		},
	)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	sourceCfg := sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")

	sinkCfg := bsinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*domain.BatchProcess[domain.CSVRow]{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	fIn := &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	fVal, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	wIn := &domain.WriteInput[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchAlias, wIn)
	require.NoError(t, err)

	q.PushBack(wVal)

	count := 0
	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId := fmt.Sprintf("batch-%d-%d", wOut.Batch.StartOffset, wOut.Batch.NextOffset)
			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

}

func writeTempCSV(t *testing.T, header []string, rows [][]string) string {
	t.Helper()

	// get current dir path
	dir, err := os.Getwd()
	require.NoError(t, err)

	fp := filepath.Join(dir, "data", "data.csv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	defer f.Close()

	write := func(cols []string) {
		for i, c := range cols {
			if i > 0 {
				_, _ = f.WriteString(",")
			}
			_, _ = f.WriteString(c)
		}
		_, _ = f.WriteString("\n")
	}

	if len(header) > 0 {
		write(header)
	}
	for _, r := range rows {
		write(r)
	}
	require.NoError(t, f.Sync())
	return fp
}
