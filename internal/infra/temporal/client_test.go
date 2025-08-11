package temporal_test

import (
	"context"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestNewTemporalClient(t *testing.T) {
	l := logger.GetSlogLogger()
	t.Log("TestNewTemporalClient Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	tCfg := envutils.BuildTemporalConfig("TestNewTemporalClient")

	// Create a new Temporal client
	tc, err := temporal.NewTemporalClient(ctx, tCfg)
	require.NoError(t, err, "Failed to create Temporal client")
	defer func() {
		err := tc.Close(ctx)
		require.NoError(t, err, "Failed to close Temporal client")
	}()

	tc.RegisterWorkflowWithAlias("TestWorkflow", "test-workflow")
	tc.RegisterActivityWithAlias("TestActivity", "test-activity")

	regWkfls := tc.RegisteredWorkflows()
	require.Equal(t, 1, len(regWkfls), "Registered workflows count should be 1")

	regActs := tc.RegisteredActivities()
	require.Equal(t, 1, len(regActs), "Registered activities count should be 1")
}
