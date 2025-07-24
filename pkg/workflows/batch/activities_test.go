package batch_test

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/hankgalt/workflow-scheduler/pkg/models/batch"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/future"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
	btchwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/batch"
	btchutils "github.com/hankgalt/workflow-scheduler/pkg/workflows/batch/utils"
)

const TEST_DIR = "data"
const DATA_PATH string = "scheduler"
const MISSING_FILE_NAME string = "geo.csv"
const LIVE_FILE_NAME string = "Agents-sm.csv"

const (
	ActivityAlias                     string = "some-random-activity-alias"
	SetupLocalCSVBatchAlias           string = "setup-local-csv-batch-alias"
	SetupCloudCSVBatchAlias           string = "setup-cloud-csv-batch-alias"
	SetupLocalCSVMongoBatchAlias      string = "setup-local-csv-mongo-batch-alias"
	SetupCSVBatchAlias                string = "setup-csv-batch-alias"
	HandleLocalCSVBatchDataAlias      string = "handle-local-csv-batch-data-alias"
	HandleCloudCSVBatchDataAlias      string = "handle-cloud-csv-batch-data-alias"
	HandleLocalCSVMongoBatchDataAlias string = "handle-local-csv-mongo-batch-data-alias"
)

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
	l := logger.GetSlogLogger()

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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: SetupLocalCSVBatchAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupLocalCSVBatch()
}

func (s *BatchActivitiesTestSuite) testSetupLocalCSVBatch() {
	l := s.GetLogger()

	req, err := buildLocalCSVBatchRequest(2, 500)
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: SetupLocalCSVBatchAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := buildLocalCSVBatchRequest(max, size)
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: SetupLocalCSVBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVBatchData, activity.RegisterOptions{Name: HandleLocalCSVBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := buildLocalCSVBatchRequest(max, size)
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
	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
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
			batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupCloudCSVBatch, activity.RegisterOptions{Name: SetupCloudCSVBatchAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupCloudCSVBatch()
}

func (s *BatchActivitiesTestSuite) testSetupCloudCSVBatch() {
	l := s.GetLogger()

	req, err := buildCloudCSVBatchRequest(2, 500)
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupCloudCSVBatch, activity.RegisterOptions{Name: SetupCloudCSVBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleCloudCSVBatchData, activity.RegisterOptions{Name: HandleCloudCSVBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := buildCloudCSVBatchRequest(max, size)
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
	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleCloudCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
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
			batResp, err := s.env.ExecuteActivity(btchwkfl.HandleCloudCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVMongoBatch, activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testSetupLocalCSVMongoBatch()
}

func (s *BatchActivitiesTestSuite) testSetupLocalCSVMongoBatch() {
	l := s.GetLogger()
	req, err := buildLocalCSVMongoBatchRequest(2, 500)
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVMongoBatch, activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVMongoBatchData, activity.RegisterOptions{Name: HandleLocalCSVMongoBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// build inital request
	max, size := uint(2), uint(500)
	req, err := buildLocalCSVMongoBatchRequest(max, size)
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
	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVMongoBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
	s.NoError(err)
	q.PushBack(batResp)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && req.End <= req.Start {
			// build next batch
			resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVMongoBatch, req.Config, req.RequestConfig)
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
			batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVMongoBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
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
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: SetupLocalCSVBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVBatchData, activity.RegisterOptions{Name: HandleLocalCSVBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleLocalCSVBatch()
}

func (s *BatchActivitiesTestSuite) testHandleLocalCSVBatch() {
	l := s.GetLogger()

	req, err := buildLocalCSVBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVBatch, req.Config, req.RequestConfig)
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

	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}

func (s *BatchActivitiesTestSuite) Test_HandleCloudCSVBatchActivity() {
	// register activities
	s.env.RegisterActivityWithOptions(btchwkfl.SetupCloudCSVBatch, activity.RegisterOptions{Name: SetupCloudCSVBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleCloudCSVBatchData, activity.RegisterOptions{Name: HandleCloudCSVBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleCloudCSVBatch()
}

func (s *BatchActivitiesTestSuite) testHandleCloudCSVBatch() {
	l := s.GetLogger()

	req, err := buildCloudCSVBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupCloudCSVBatch, req.Config, req.RequestConfig)
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

	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleCloudCSVBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}

func (s *BatchActivitiesTestSuite) Test_HandleLocalCSVMongoBatchDataActivity() {
	// register SetupLocalCSVMongoBatchActivity & HandleLocalCSVMongoBatchDataActivity
	s.env.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVMongoBatch, activity.RegisterOptions{Name: SetupLocalCSVMongoBatchAlias})
	s.env.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVMongoBatchData, activity.RegisterOptions{Name: HandleLocalCSVMongoBatchDataAlias})

	// set scheduler client in test environment context
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
	})

	// test for csv file read headers
	s.testHandleLocalCSVMongoBatchData()
}

func (s *BatchActivitiesTestSuite) testHandleLocalCSVMongoBatchData() {
	l := s.GetLogger()

	req, err := buildLocalCSVMongoBatchRequest(2, 500)
	s.NoError(err)

	// setup batch map
	if req.Batches == nil {
		req.Batches = make(map[string]*batch.Batch)
	}

	l.Debug("csv file read headers request", "req", req)
	resp, err := s.env.ExecuteActivity(btchwkfl.SetupLocalCSVMongoBatch, req.Config, req.RequestConfig)
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

	batResp, err := s.env.ExecuteActivity(btchwkfl.HandleLocalCSVMongoBatchData, req.Config, req.RequestConfig, req.Batches[batchID])
	s.NoError(err)

	var batchResult *batch.Batch
	err = batResp.Get(&batchResult)
	s.NoError(err)
	l.Debug("csv file read headers result", "batchResult", batchResult)
}

func getMongoTestConfig() batch.MongoBatchConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbHost := os.Getenv("MONGO_HOSTNAME")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbParams := os.Getenv("MONGO_CONN_PARAMS")
	dbName := os.Getenv("MONGO_DBNAME")
	return batch.MongoBatchConfig{
		Protocol: dbProtocol,
		Host:     dbHost,
		User:     dbUser,
		Pwd:      dbPwd,
		Params:   dbParams,
		Name:     dbName,
	}
}

// buildLocalCSVBatchRequest builds a LocalCSVBatchRequest with the given max batches and batch size.
// It uses the DATA_DIR environment variable to determine the data path, defaulting to TEST_DIR
// if DATA_DIR is not set. It also checks if the file exists at the specified path.
// If the file does not exist, it returns an error.
func buildLocalCSVBatchRequest(max, size uint) (*batch.LocalCSVBatchRequest, error) {
	filePath, err := getFilePath()
	if err != nil {
		return nil, err
	}
	fileName := getFileName()

	req := &batch.LocalCSVBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: batch.LocalCSVBatchConfig{
			Name: fileName,
			Path: filePath,
		},
	}

	return req, nil
}

func buildCloudCSVBatchRequest(max, size uint) (*batch.CloudCSVBatchRequest, error) {
	filePath := DATA_PATH
	fileName := getFileName()

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("BUCKET environment variable is not set")
	}

	req := &batch.CloudCSVBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: batch.CloudCSVBatchConfig{
			Name:   fileName,
			Path:   filePath,
			Bucket: bucket,
		},
	}

	return req, nil
}

func buildLocalCSVMongoBatchRequest(max, size uint) (*batch.LocalCSVMongoBatchRequest, error) {
	filePath, err := getFilePath()
	if err != nil {
		return nil, err
	}
	fileName := getFileName()

	mCfg := getMongoTestConfig()

	req := &batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: max,
				BatchSize:  size,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: batch.LocalCSVMongoBatchConfig{
			LocalCSVBatchConfig: batch.LocalCSVBatchConfig{
				Name: fileName,
				Path: filePath,
			},
			MongoBatchConfig: mCfg,
		},
	}

	return req, nil
}

func getFilePath() (string, error) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = TEST_DIR
		fmt.Printf("DATA_DIR environment variable is not set, using default: %s\n", TEST_DIR)
	}

	filePath := fmt.Sprintf("%s/%s", dataDir, DATA_PATH)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("Data path does not exist: %s\n", filePath)
		return "", fmt.Errorf("data path does not exist: %s", filePath)
	}

	return filePath, nil
}

func getFileName() string {
	fileName := os.Getenv("LIVE_FILE_NAME")
	if fileName == "" {
		fileName = LIVE_FILE_NAME
		fmt.Printf("LIVE_FILE_NAME environment variable is not set, using default: %s\n", LIVE_FILE_NAME)
	}

	return fileName
}
