package sandbox_test

import (
	"context"
	"errors"
	"log"
	"math"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"
	sdwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/sandbox"
	"github.com/stretchr/testify/require"
)

// SDK test suite cannot propagate ContinueAsNewError to a new run.
// SDK test suite test to test propagation of workflow as continue-as-new through
// ContinueAsNewError on workflow reaching specified event limit in first run.
func Test_ContinueAsNew_ZeroStart(t *testing.T) {
	// get test logger
	l := logger.GetSlogLogger()

	// setup context with logger
	ctx := logger.WithLogger(context.Background(), l)

	// Setup new test suite environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Setup the worker options
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	env.RegisterWorkflow(sdwkfl.ContinueAsNewForBatch)

	// input arg1
	start := &sdwkfl.ContinueAsNewForBatchInput{
		Counter:  0,
		RunCount: 0,
	}

	// Setup contextual batch counter limit (input arg2) & workflow counter limit (input arg3)
	batchLimit, eventlimit := uint32(5), uint64(25)

	// Execute the workflow
	env.ExecuteWorkflow(sdwkfl.ContinueAsNewForBatch, start, batchLimit, eventlimit)

	// Assertions
	require.True(t, env.IsWorkflowCompleted(), "Workflow should be completed")
	require.True(t, env.IsWorkflowCompleted())

	// Expect the sentinel error since TestWorkflowEnvironment runs a single workflow run & fails on ContinueAsNewError.
	err := env.GetWorkflowError()
	var ca *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)

	// Verify error details
	require.Equal(t, sdwkfl.ContinueAsNewForBatchWorkflow, ca.WorkflowType.Name, "expected workflow type to match")
	switch wkflErr := err.(type) {
	case *temporal.WorkflowExecutionError:
		log.Println("ContinueAsNewForBatch - temporal server WorkflowExecutionError", "error-details", wkflErr)
	default:
		log.Println("ContinueAsNewForBatch - unexpected error type", "error-details", wkflErr)
	}
}

// Integration test for ContinueAsNew propagation on a dev Temporal server.
// Start temporal dev server before running this test.
func Test_ContinueAsNew_Integration_Completes(t *testing.T) {
	// get test logger
	l := logger.GetSlogLogger()

	// setup context with logger
	ctx := logger.WithLogger(context.Background(), l)

	// Temporal client connection
	c, err := client.Dial(
		client.Options{
			HostPort: client.DefaultHostPort,
			Logger:   l,
		},
	)
	if err != nil {
		l.Error("Temporal dev server not running (localhost:7233)", "error", err)
		return
	}
	defer c.Close()

	// workflow task queue
	const tq = "continue-as-new-for-batch-tq"

	// Temporal worker for the task queue
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: ctx,            // Global worker context for activities
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	w.RegisterWorkflow(sdwkfl.ContinueAsNewForBatch)

	// Start the worker in a goroutine
	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Println("worker stopped:", err)
		}
	}()

	// Setup contextual workflow event limit & total count
	wkflEventLimit, totalCount := uint32(5), uint64(27)

	// Create workflow execution context
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	// Kick off the workflow.
	run, err := c.ExecuteWorkflow(
		ctx, // context
		client.StartWorkflowOptions{ // workflow options
			ID:        "casn-it-" + time.Now().Format("150405.000"),
			TaskQueue: tq,
		},
		sdwkfl.ContinueAsNewForBatch, // workflow function
		&sdwkfl.ContinueAsNewForBatchInput{ // workflow input arg1
			Counter:  0,
			RunCount: 0,
		},
		wkflEventLimit, // workflow input arg2
		totalCount,     // workflow input arg3
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out *sdwkfl.ContinueAsNewForBatchInput
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)

	roundedUp := math.Ceil(float64(totalCount) / float64(wkflEventLimit))

	l.Debug("ContinueAsNewForBatch - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"counter", out.Counter,
		"run-count", out.RunCount,
		"total-count", totalCount,
		"rounded-up", roundedUp,
	)

	require.EqualValues(t, totalCount, out.Counter)
	require.EqualValues(t, roundedUp, out.RunCount)
}
