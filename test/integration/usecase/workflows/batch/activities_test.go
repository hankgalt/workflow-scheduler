package batch_test

import (
	"container/list"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/future"
)

const (
	ActivityAlias                     string = "some-random-activity-alias"
	SetupCSVBatchAlias                string = "setup-csv-batch-alias"
	SetupLocalCSVBatchAlias           string = "setup-local-csv-batch-alias"
	SetupCloudCSVBatchAlias           string = "setup-cloud-csv-batch-alias"
	SetupLocalCSVMongoBatchAlias      string = "setup-local-csv-mongo-batch-alias"
	SetupCloudCSVMongoBatchAlias      string = "setup-cloud-csv-mongo-batch-alias"
	HandleLocalCSVBatchDataAlias      string = "handle-local-csv-batch-data-alias"
	HandleCloudCSVBatchDataAlias      string = "handle-cloud-csv-batch-data-alias"
	HandleLocalCSVMongoBatchDataAlias string = "handle-local-csv-mongo-batch-data-alias"
	HandleCloudCSVMongoBatchDataAlias string = "handle-cloud-csv-mongo-batch-data-alias"
)

var agentHeaderMapping = map[string]string{
	"ENTITY_NUM":       "ENTITY_ID",
	"PHYSICAL_ADDRESS": "ADDRESS",
}

type BatchActivitiesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestActivityEnvironment
}

func TestBatchActivitiesTestSuite(t *testing.T) {
	suite.Run(t, new(BatchActivitiesTestSuite))
}

func (s *BatchActivitiesTestSuite) SetupTest() {
	// get test logger
	l := logger.GetSlogMultiLogger("data")

	// set environment logger
	s.SetLogger(l)

	// set test activity environment
	s.env = s.NewTestActivityEnvironment()
}

func (s *BatchActivitiesTestSuite) TearDownTest() {
	// s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *BatchActivitiesTestSuite) Test_LocalActivity() {
	l := s.GetLogger()

	localActivityFn := func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}

	resp, err := s.env.ExecuteLocalActivity(localActivityFn, "local_activity")
	s.NoError(err)

	var result string
	err = resp.Get(&result)
	s.NoError(err)

	l.Debug("activity result", "result", result)
	s.Equal("hello local_activity", result)
}

func (s *BatchActivitiesTestSuite) Test_ActivityRegistration() {
	l := s.GetLogger()

	activityFn := func(msg string) (string, error) {
		return msg, nil
	}

	s.env.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: ActivityAlias})

	input := "some random input"
	encodedValue, err := s.env.ExecuteActivity(activityFn, input)
	s.NoError(err)

	var output string
	err = encodedValue.Get(&output)
	s.NoError(err)
	s.Equal(input, output)

	encodedValue, err = s.env.ExecuteActivity(ActivityAlias, input)
	s.NoError(err)

	output = ""
	err = encodedValue.Get(&output)
	s.NoError(err)

	l.Debug("activity result", "output", output)
	s.Equal(input, output)
}

func (s *BatchActivitiesTestSuite) Test_SetupLocalCSVBatchActivity() {
	// register SetupLocalCSVBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVBatch,
		activity.RegisterOptions{Name: SetupLocalCSVBatchAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupLocalCSVBatch()
}

func (s *BatchActivitiesTestSuite) testSetupLocalCSVBatch() {
	l := s.GetLogger()

	req, err := envutils.BuildLocalCSVBatchRequest(2, 500)
	s.NoError(err)

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	for req.End <= req.Start {
		resp, err = s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
		s.NoError(err)

		err = resp.Get(&result)
		s.NoError(err)
		l.Debug("csv file read headers result", "offsets", result.Offsets)
		req.RequestConfig = &result
	}
	l.Debug("csv file read headers final result", "num-offsets", len(req.Offsets), "headers", req.Headers)
}

func (s *BatchActivitiesTestSuite) Test_LocalCSVBatchActivityQueue_DummyFuture() {
	l := s.GetLogger()

	type BuildBatch func(string) batch.Batch
	buildBatch := func(batchId string) batch.Batch {
		toks := strings.Split(batchId, "-")
		if len(toks) > 3 {
			start, err := strconv.Atoi(toks[len(toks)-2])
			if err != nil {
				return batch.Batch{}
			}
			end, err := strconv.Atoi(toks[len(toks)-1])
			if err != nil {
				return batch.Batch{}
			}
			return batch.Batch{
				BatchID: batchId,
				Start:   uint64(start),
				End:     uint64(end),
			}
		}
		return batch.Batch{}
	}

	// register SetupLocalCSVBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVBatch,
		activity.RegisterOptions{Name: SetupLocalCSVBatchAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := envutils.BuildLocalCSVBatchRequest(max, size)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	// setup first batch
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("first batch setup result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	// build first batch request
	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)
	l.Debug("generated first batch ID", "batchID", batchID, "start", start, "end", end)
	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	// execute future & push to queue
	fut := future.NewDummyFuture(time.Second*2, buildBatch, batchID)
	s.False(fut.IsReady(), "Future should not be ready yet")
	q.PushBack(fut)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
			s.NoError(err)

			var result batch.RequestConfig
			err = resp.Get(&result)
			s.NoError(err)
			l.Debug("next batch setup result", "offsets", result.Offsets)
			req.RequestConfig = &result

			// build next batch request
			start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
			batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
			s.NoError(err)
			l.Debug("generated next batch ID", "batchID", batchID, "start", start, "end", end)
			if _, ok := req.Batches[batchID]; !ok {
				batch := &batch.Batch{
					BatchID: batchID,
					Start:   start,
					End:     end,
				}

				// update request batch state
				req.Batches[batchID] = batch
			}

			// execute future & push to queue
			fut := future.NewDummyFuture(time.Second*2, buildBatch, batchID)
			s.False(fut.IsReady(), "Future should not be ready yet")
			q.PushBack(fut)
		} else {
			fut := q.Remove(q.Front()).(future.Future[BuildBatch, string, batch.Batch])

			var result batch.Batch
			err := fut.Get(context.Background(), fut.Arg(), &result)
			s.NoError(err)

			if req.End > req.Start && result.End == req.End {
				l.Debug("processed last batch", "batchID", fut.Arg(), "result", result)
			} else {
				l.Debug("processed batch", "batchID", fut.Arg(), "result", result)
			}
		}
	}
}

func (s *BatchActivitiesTestSuite) Test_LocalCSVBatchActivityQueue() {
	l := s.GetLogger()

	// register SetupLocalCSVBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: SetupLocalCSVBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleLocalCSVBatchData, activity.RegisterOptions{Name: HandleLocalCSVBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := envutils.BuildLocalCSVBatchRequest(max, size)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	// setup first batch
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("first batch setup result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	// build first batch request
	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)
	l.Debug("generated first batch ID", "batchID", batchID, "start", start, "end", end)
	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	// execute future & push to queue
	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleLocalCSVBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)
	q.PushBack(batResp)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
			s.NoError(err)

			var result batch.RequestConfig
			err = resp.Get(&result)
			s.NoError(err)
			l.Debug("next batch setup result", "offsets", result.Offsets)
			req.RequestConfig = &result

			// build next batch request
			start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
			batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
			s.NoError(err)
			l.Debug("generated next batch ID", "batchID", batchID, "start", start, "end", end)
			if _, ok := req.Batches[batchID]; !ok {
				batch := &batch.Batch{
					BatchID: batchID,
					Start:   start,
					End:     end,
				}

				// update request batch state
				req.Batches[batchID] = batch
			}

			// execute future & push to queue
			batResp, err := s.env.ExecuteActivity(
				btchwkfl.HandleLocalCSVBatchData,
				req.Config,
				req.RequestConfig,
				req.Batches[batchID],
			)
			s.NoError(err)
			q.PushBack(batResp)
		} else {
			batResp := q.Remove(q.Front()).(converter.EncodedValue)
			var batchResult *batch.Batch
			err = batResp.Get(&batchResult)
			s.NoError(err)

			if req.End > req.Start && batchResult.End == req.End {
				l.Debug("processed last batch", "batchID", batchResult.BatchID, "result", batchResult)
			} else {
				l.Debug("processed batch", "batchID", batchResult.BatchID, "result", batchResult)
			}
		}
	}

}

func (s *BatchActivitiesTestSuite) Test_SetupCloudCSVBatchActivity() {
	// register SetupCloudCSVBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVBatch,
		activity.RegisterOptions{Name: SetupCloudCSVBatchAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupCloudCSVBatch()
}

func (s *BatchActivitiesTestSuite) testSetupCloudCSVBatch() {
	l := s.GetLogger()

	req, err := envutils.BuildCloudCSVBatchRequest(2, 500)
	s.NoError(err)

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)

	for req.End <= req.Start {
		resp, err = s.env.ExecuteActivity(btchwkfl.SetupCloudCSVBatch, req.Config, req.RequestConfig)
		s.NoError(err)

		err = resp.Get(&result)
		s.NoError(err)
		l.Debug("csv file read headers result", "offsets", result.Offsets)
		req.RequestConfig = &result
	}
	l.Debug("csv file read headers final result", "num-offsets", len(req.Offsets), "headers", req.Headers)
}

func (s *BatchActivitiesTestSuite) Test_CloudCSVBatchActivityQueue() {
	l := s.GetLogger()

	// register SetupCloudCSVBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVBatch,
		activity.RegisterOptions{Name: SetupCloudCSVBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleCloudCSVBatchData,
		activity.RegisterOptions{Name: HandleCloudCSVBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := envutils.BuildCloudCSVBatchRequest(max, size)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	// setup first batch
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("first batch setup result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	// build first batch request
	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)
	l.Debug("generated first batch ID", "batchID", batchID, "start", start, "end", end)
	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	// execute future & push to queue
	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleCloudCSVBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)
	q.PushBack(batResp)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVBatch, req.Config, req.RequestConfig)
			s.NoError(err)

			var result batch.RequestConfig
			err = resp.Get(&result)
			s.NoError(err)
			l.Debug("next batch setup result", "offsets", result.Offsets)
			req.RequestConfig = &result

			// build next batch request
			start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
			batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
			s.NoError(err)
			l.Debug("generated next batch ID", "batchID", batchID, "start", start, "end", end)
			if _, ok := req.Batches[batchID]; !ok {
				batch := &batch.Batch{
					BatchID: batchID,
					Start:   start,
					End:     end,
				}

				// update request batch state
				req.Batches[batchID] = batch
			}

			// execute future & push to queue
			batResp, err := s.env.ExecuteActivity(
				btchwkfl.HandleCloudCSVBatchData,
				req.Config,
				req.RequestConfig,
				req.Batches[batchID],
			)
			s.NoError(err)
			q.PushBack(batResp)
		} else {
			batResp := q.Remove(q.Front()).(converter.EncodedValue)
			var batchResult *batch.Batch
			err = batResp.Get(&batchResult)
			s.NoError(err)

			if req.End > req.Start && batchResult.End == req.End {
				l.Debug("processed last batch", "batchID", batchResult.BatchID, "result", batchResult)
			} else {
				l.Debug("processed batch", "batchID", batchResult.BatchID, "result", batchResult)
			}
		}
	}

}

func (s *BatchActivitiesTestSuite) Test_SetupLocalCSVMongoBatchActivity() {
	// register SetupLocalCSVMongoBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVMongoBatch,
		activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupLocalCSVMongoBatch()
}

func (s *BatchActivitiesTestSuite) testSetupLocalCSVMongoBatch() {
	l := s.GetLogger()
	req, err := envutils.BuildLocalCSVMongoBatchRequest(2, 500)
	s.NoError(err)

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVMongoBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)

	// update request config
	req.RequestConfig = &result

	for req.End <= req.Start {
		resp, err = s.env.ExecuteActivity(btchwkfl.SetupLocalCSVMongoBatch, req.Config, req.RequestConfig)
		s.NoError(err)

		err = resp.Get(&result)
		s.NoError(err)
		l.Debug("csv file read result", "offsets", result.Offsets)
		req.RequestConfig = &result
	}
	l.Debug("csv file read headers final result", "num-offsets", len(req.Offsets), "headers", req.Headers)
}

func (s *BatchActivitiesTestSuite) Test_LocalCSVMongoBatchActivityQueue() {
	l := s.GetLogger()

	// register SetupLocalCSVMongoBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVMongoBatch,
		activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleLocalCSVMongoBatchData,
		activity.RegisterOptions{Name: HandleLocalCSVMongoBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := envutils.BuildLocalCSVMongoBatchRequest(max, size)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	// setup first batch
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVMongoBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("first batch setup result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	// build first batch request
	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)
	l.Debug("generated first batch ID", "batchID", batchID, "start", start, "end", end)
	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	// execute future & push to queue
	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleLocalCSVMongoBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)
	q.PushBack(batResp)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(
				btchwkfl.SetupLocalCSVMongoBatch,
				req.Config,
				req.RequestConfig,
			)
			s.NoError(err)

			var result batch.RequestConfig
			err = resp.Get(&result)
			s.NoError(err)
			l.Debug("next batch setup result", "offsets", result.Offsets)
			req.RequestConfig = &result

			// build next batch request
			start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
			batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
			s.NoError(err)
			l.Debug("generated next batch ID", "batchID", batchID, "start", start, "end", end)
			if _, ok := req.Batches[batchID]; !ok {
				batch := &batch.Batch{
					BatchID: batchID,
					Start:   start,
					End:     end,
				}

				// update request batch state
				req.Batches[batchID] = batch
			}

			// execute future & push to queue
			batResp, err := s.env.ExecuteActivity(
				btchwkfl.HandleLocalCSVMongoBatchData,
				req.Config,
				req.RequestConfig,
				req.Batches[batchID],
			)
			s.NoError(err)
			q.PushBack(batResp)
		} else {
			batResp := q.Remove(q.Front()).(converter.EncodedValue)
			var batchResult *batch.Batch
			err = batResp.Get(&batchResult)
			s.NoError(err)

			if req.End > req.Start && batchResult.End == req.End {
				l.Debug("processed last batch", "batchID", batchResult.BatchID, "result", batchResult)
			} else {
				l.Debug("processed batch", "batchID", batchResult.BatchID, "result", batchResult)
			}
		}
	}
}

func (s *BatchActivitiesTestSuite) Test_SetupCloudCSVMongoBatchActivity() {
	// register SetupCloudCSVMongoBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVMongoBatch,
		activity.RegisterOptions{Name: SetupCloudCSVMongoBatchAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupCloudCSVMongoBatch()
}

func (s *BatchActivitiesTestSuite) testSetupCloudCSVMongoBatch() {
	l := s.GetLogger()
	req, err := envutils.BuildCloudCSVMongoBatchRequest(2, 500, agentHeaderMapping)
	s.NoError(err)

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVMongoBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)

	// update request config
	req.RequestConfig = &result

	for req.End <= req.Start {
		resp, err = s.env.ExecuteActivity(btchwkfl.SetupCloudCSVMongoBatch, req.Config, req.RequestConfig)
		s.NoError(err)

		err = resp.Get(&result)
		s.NoError(err)
		l.Debug("csv file read result", "offsets", result.Offsets)
		req.RequestConfig = &result
	}
	l.Debug("csv file read headers final result", "num-offsets", len(req.Offsets), "headers", req.Headers)
}

func (s *BatchActivitiesTestSuite) Test_CloudCSVMongoBatchActivityQueue() {
	l := s.GetLogger()

	// register SetupCloudCSVMongoBatchActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVMongoBatch,
		activity.RegisterOptions{Name: SetupCloudCSVMongoBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleCloudCSVMongoBatchData,
		activity.RegisterOptions{Name: HandleCloudCSVMongoBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := envutils.BuildCloudCSVMongoBatchRequest(max, size, agentHeaderMapping)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	// initiate a new queue
	q := list.New()

	// setup first batch
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVMongoBatch, req.Config, req.RequestConfig)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("first batch setup result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	// build first batch request
	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)
	l.Debug("generated first batch ID", "batchID", batchID, "start", start, "end", end)
	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	// execute future & push to queue
	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleCloudCSVMongoBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)
	q.PushBack(batResp)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(
				btchwkfl.SetupCloudCSVMongoBatch,
				req.Config,
				req.RequestConfig,
			)
			s.NoError(err)

			var result batch.RequestConfig
			err = resp.Get(&result)
			s.NoError(err)
			l.Debug("next batch setup result", "offsets", result.Offsets)
			req.RequestConfig = &result

			// build next batch request
			start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
			batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
			s.NoError(err)
			l.Debug("generated next batch ID", "batchID", batchID, "start", start, "end", end)
			if _, ok := req.Batches[batchID]; !ok {
				batch := &batch.Batch{
					BatchID: batchID,
					Start:   start,
					End:     end,
				}

				// update request batch state
				req.Batches[batchID] = batch
			}

			// execute future & push to queue
			batResp, err := s.env.ExecuteActivity(
				btchwkfl.HandleCloudCSVMongoBatchData,
				req.Config,
				req.RequestConfig,
				req.Batches[batchID],
			)
			s.NoError(err)
			q.PushBack(batResp)
		} else {
			batResp := q.Remove(q.Front()).(converter.EncodedValue)
			var batchResult *batch.Batch
			err = batResp.Get(&batchResult)
			s.NoError(err)

			if req.End > req.Start && batchResult.End == req.End {
				l.Debug("processed last batch", "batchID", batchResult.BatchID, "result", batchResult)
			} else {
				l.Debug("processed batch", "batchID", batchResult.BatchID, "result", batchResult)
			}
		}
	}
}

func (s *BatchActivitiesTestSuite) Test_HandleLocalCSVBatchActivity() {
	// register activities
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVBatch,
		activity.RegisterOptions{Name: SetupLocalCSVBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleLocalCSVBatchData,
		activity.RegisterOptions{Name: HandleLocalCSVBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleLocalCSVBatch()
}

func (s *BatchActivitiesTestSuite) testHandleLocalCSVBatch() {
	l := s.GetLogger()

	req, err := envutils.BuildLocalCSVBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(
		btchwkfl.SetupLocalCSVBatch,
		req.Config,
		req.RequestConfig,
	)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)

	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleLocalCSVBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}

func (s *BatchActivitiesTestSuite) Test_HandleCloudCSVBatchActivity() {
	// register activities
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVBatch,
		activity.RegisterOptions{Name: SetupCloudCSVBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleCloudCSVBatchData,
		activity.RegisterOptions{Name: HandleCloudCSVBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleCloudCSVBatch()
}

func (s *BatchActivitiesTestSuite) testHandleCloudCSVBatch() {
	l := s.GetLogger()

	req, err := envutils.BuildCloudCSVBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(
		btchwkfl.SetupCloudCSVBatch,
		req.Config,
		req.RequestConfig,
	)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)
	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)

	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleCloudCSVBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}

func (s *BatchActivitiesTestSuite) Test_HandleLocalCSVMongoBatchDataActivity() {
	// register SetupLocalCSVMongoBatchActivity & HandleLocalCSVMongoBatchDataActivity
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVMongoBatch,
		activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleLocalCSVMongoBatchData,
		activity.RegisterOptions{Name: HandleLocalCSVMongoBatchDataAlias},
	)

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleLocalCSVMongoBatchData()
}

func (s *BatchActivitiesTestSuite) testHandleLocalCSVMongoBatchData() {
	l := s.GetLogger()

	req, err := envutils.BuildLocalCSVMongoBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(
		btchwkfl.SetupLocalCSVMongoBatch,
		req.Config,
		req.RequestConfig,
	)
	s.NoError(err)

	var result batch.RequestConfig
	err = resp.Get(&result)
	s.NoError(err)

	l.Debug("csv file read headers result", "offsets", result.Offsets, "headers", result.Headers)
	req.RequestConfig = &result

	start, end := req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1]
	batchID, err := btchutils.GenerateBatchID(req.Config, start, end)
	s.NoError(err)

	if _, ok := req.Batches[batchID]; !ok {
		batch := &batch.Batch{
			BatchID: batchID,
			Start:   start,
			End:     end,
		}

		// update request batch state
		req.Batches[batchID] = batch
	}

	batResp, err := s.env.ExecuteActivity(
		btchwkfl.HandleLocalCSVMongoBatchData,
		req.Config,
		req.RequestConfig,
		req.Batches[batchID],
	)
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}
