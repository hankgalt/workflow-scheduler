package sandbox_test

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	sdwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/sandbox"
	"github.com/stretchr/testify/require"
)

// SDK test suite cannot propagate ContinueAsNewError to a new run,
// so we test the sentinel(conditions where ContinueAsNewError is expected) case here.
func Test_ContinueAsNew_ZeroStart(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.RegisterWorkflow(sdwkfl.ContinueAsNewForBatch)

	start := &sdwkfl.ContinueAsNewForBatchInput{Counter: 0, RunCount: 0}
	env.ExecuteWorkflow(sdwkfl.ContinueAsNewForBatch, start)

	require.True(t, env.IsWorkflowCompleted())

	// Expect the sentinel since TestWorkflowEnvironment runs a single workflow run.
	err := env.GetWorkflowError()
	var ca *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)

	switch wkflErr := err.(type) {
	case *temporal.WorkflowExecutionError:
		log.Println("ContinueAsNewForBatch - temporal server WorkflowExecutionError", "error", wkflErr.Error(), "type", wkflErr)
	default:
		log.Println("ContinueAsNewForBatch - unexpected error type", "error", wkflErr.Error(), "type", wkflErr)
	}
}

// This test checks that the ContinueAsNew propagation on dev server
// Start temporal dev server before running this test.
func Test_ContinueAsNew_Integration_Completes(t *testing.T) {
	c, err := client.Dial(client.Options{HostPort: client.DefaultHostPort})
	if err != nil {
		t.Skipf("Temporal dev server not running (localhost:7233): %v", err)
	}
	defer c.Close()

	// Spin up a worker for this test.
	const tq = "continue-as-new-it"
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: context.Background(),
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})
	w.RegisterWorkflow(sdwkfl.ContinueAsNewForBatch)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Println("worker stopped:", err)
		}
	}()

	// Kick off the workflow.
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "casn-it-" + time.Now().Format("150405.000"),
		TaskQueue: tq,
	}, sdwkfl.ContinueAsNewForBatch, &sdwkfl.ContinueAsNewForBatchInput{Counter: 0, RunCount: 0})
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out *sdwkfl.ContinueAsNewForBatchInput
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)

	require.EqualValues(t, 25, out.Counter)
	require.EqualValues(t, 5, out.RunCount)
}
