package mongostore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/pkg/stores/mongostore"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

func TestMongoStore(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	t.Log("TestMongoStore Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	nmCfg := mongostore.GetMongoConfig()
	cl, err := mongostore.GetMongoStore(ctx, nmCfg)
	require.NoError(t, err)

	defer func() {
		err := cl.Close(ctx)
		require.NoError(t, err)
	}()
}
