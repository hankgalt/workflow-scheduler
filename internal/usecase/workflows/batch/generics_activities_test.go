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

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

const (
	FetchNextLocalCSVBatchAlias string = "fetch-next-local-csv-batch-alias"
	FetchNextCloudCSVBatchAlias string = "fetch-next-cloud-csv-batch-alias"
	WriteNoopSinkBatchAlias     string = "write-noop-sink-batch-alias"
	WriteMongoSinkBatchAlias    string = "write-mongo-sink-batch-alias"
)

type ETLRequest[T any] struct {
	MaxBatches uint
	BatchSize  uint
	Done       bool
	Offsets    []uint64
	Batches    map[string]*batch.BatchProcess[T]
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
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVBatchAlias,
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
	cfg := batch.LocalCSVConfig{
		Path:      path,
		HasHeader: true,
	}

	// initialize batch size & next offset
	// Use a batch size larger than the largest row size in bytes to ensure more than one row is fetched.
	batchSize := uint(30)
	nextOffset := uint64(0)

	// Fetch the next batch of records till done
	var out batch.FetchOutput[batch.CSVRow]
	for out.Batch.Done == false {
		fIn := batch.FetchInput[batch.CSVRow, batch.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(FetchNextLocalCSVBatchAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		if nextOffset == 0 {
			require.Len(t, out.Batch.Records, 2)
			require.EqualValues(t, 23, out.Batch.NextOffset)
			require.False(t, out.Batch.Done)
			require.Equal(t, batch.CSVRow{"id": "1", "name": "alpha"}, out.Batch.Records[0].Data)
			require.Equal(t, batch.CSVRow{"id": "2", "name": "beta"}, out.Batch.Records[1].Data)
		} else {
			require.Len(t, out.Batch.Records, 3)
			require.EqualValues(t, 49, out.Batch.NextOffset)
			require.True(t, out.Batch.Done)
			require.Equal(t, "3", out.Batch.Records[0].Data["id"])
			require.Equal(t, "gamma", out.Batch.Records[0].Data["name"])
			require.Equal(t, "5", out.Batch.Records[2].Data["id"])
			require.Equal(t, "epsilon", out.Batch.Records[2].Data["name"])
		}
		nextOffset = out.Batch.NextOffset
	}
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
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVBatchAlias,
		},
	)

	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building file path for test CSV")

	path := filepath.Join(filePath, fileName)

	cfg := batch.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: batch.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	var out batch.FetchOutput[batch.CSVRow]
	for out.Batch.Done == false {
		fIn := batch.FetchInput[batch.CSVRow, batch.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(FetchNextLocalCSVBatchAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		nextOffset = out.Batch.NextOffset
	}

	require.Equal(t, true, nextOffset > 0, "next offset should be greater than 0")
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

	// Register the generic activity instantiation we will call.
	// In test env, this is optional for function activities, but explicit is fine.
	env.RegisterActivityWithOptions(
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.CloudCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextCloudCSVBatchAlias,
		},
	)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	cfg := batch.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(batch.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: batch.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	var out batch.FetchOutput[batch.CSVRow]
	for out.Batch.Done == false {
		fIn := batch.FetchInput[batch.CSVRow, batch.CloudCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(FetchNextCloudCSVBatchAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		nextOffset = out.Batch.NextOffset
	}

	require.Equal(t, true, nextOffset > 0, "next offset should be greater than 0")
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
		btchwkfl.WriteActivity[batch.CSVRow, batch.NoopSinkConfig[batch.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNoopSinkBatchAlias,
		},
	)

	recs := []*batch.BatchRecord[batch.CSVRow]{
		{
			Start: 0,
			End:   10,
			Data:  batch.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: 10,
			End:   20,
			Data:  batch.CSVRow{"id": "2", "name": "beta"},
		},
	}
	b := &batch.BatchProcess[batch.CSVRow]{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	in := batch.WriteInput[batch.CSVRow, batch.NoopSinkConfig[batch.CSVRow]]{
		Sink:  batch.NoopSinkConfig[batch.CSVRow]{},
		Batch: b,
	}

	val, err := env.ExecuteActivity(WriteNoopSinkBatchAlias, in)
	require.NoError(t, err)

	var out batch.WriteOutput[batch.CSVRow]
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
	})

	env.RegisterActivityWithOptions(
		btchwkfl.WriteActivity[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]],
		activity.RegisterOptions{
			Name: WriteMongoSinkBatchAlias,
		},
	)

	recs := []*batch.BatchRecord[batch.CSVRow]{
		{
			Start: 0,
			End:   10,
			Data:  batch.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: 10,
			End:   20,
			Data:  batch.CSVRow{"id": "2", "name": "beta"},
		},
	}

	b := &batch.BatchProcess[batch.CSVRow]{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	nmCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, nmCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, nmCfg.Host(), "MongoDB host should not be empty")

	cfg := batch.MongoSinkConfig[batch.CSVRow]{
		Protocol:   nmCfg.Protocol(),
		Host:       nmCfg.Host(),
		DBName:     nmCfg.Name(),
		User:       nmCfg.User(),
		Pwd:        nmCfg.Pwd(),
		Params:     nmCfg.Params(),
		Collection: "test.people",
	}

	in := batch.WriteInput[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]]{
		Sink:  cfg,
		Batch: b,
	}

	val, err := env.ExecuteActivity(WriteMongoSinkBatchAlias, in)
	require.NoError(t, err)

	var out batch.WriteOutput[batch.CSVRow]
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
	})

	// Register activities.
	env.RegisterActivityWithOptions(
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		btchwkfl.WriteActivity[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]],
		activity.RegisterOptions{
			Name: WriteMongoSinkBatchAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := batch.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: batch.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := batch.MongoSinkConfig[batch.CSVRow]{
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
	etlReq := &ETLRequest[batch.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*batch.BatchProcess[batch.CSVRow]{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := batch.FetchInput[batch.CSVRow, batch.LocalCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(FetchNextLocalCSVBatchAlias, fIn)
	require.NoError(t, err)

	var fOut batch.FetchOutput[batch.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := batch.WriteInput[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(WriteMongoSinkBatchAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn := batch.FetchInput[batch.CSVRow, batch.LocalCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(FetchNextLocalCSVBatchAlias, fIn)
			require.NoError(t, err)

			var fOut batch.FetchOutput[batch.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn := batch.WriteInput[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(WriteMongoSinkBatchAlias, wIn)
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
			var wOut batch.WriteOutput[batch.CSVRow]
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
	})

	// Register concrete generic instantiations used by the test.
	env.RegisterActivityWithOptions(
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.CloudCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextCloudCSVBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		btchwkfl.WriteActivity[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]],
		activity.RegisterOptions{
			Name: WriteMongoSinkBatchAlias,
		},
	)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	sourceCfg := batch.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(batch.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: batch.BuildBusinessModelTransformRules(),
	}

	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")

	sinkCfg := batch.MongoSinkConfig[batch.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[batch.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*batch.BatchProcess[batch.CSVRow]{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	fIn := batch.FetchInput[batch.CSVRow, batch.CloudCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	fVal, err := env.ExecuteActivity(FetchNextCloudCSVBatchAlias, fIn)
	require.NoError(t, err)

	var fOut batch.FetchOutput[batch.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	wIn := batch.WriteInput[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	wVal, err := env.ExecuteActivity(WriteMongoSinkBatchAlias, wIn)
	require.NoError(t, err)

	q.PushBack(wVal)

	count := 0
	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn := batch.FetchInput[batch.CSVRow, batch.CloudCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(FetchNextCloudCSVBatchAlias, fIn)
			require.NoError(t, err)

			var fOut batch.FetchOutput[batch.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn := batch.WriteInput[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(WriteMongoSinkBatchAlias, wIn)
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
			var wOut batch.WriteOutput[batch.CSVRow]
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
