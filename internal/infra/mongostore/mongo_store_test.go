package mongostore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestMongoStore(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	t.Log("TestMongoStore Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	nmCfg := envutils.BuildMongoStoreConfig()
	cl, err := mongostore.NewMongoStore(ctx, nmCfg)
	require.NoError(t, err)

	defer func() {
		err := cl.Close(ctx)
		require.NoError(t, err)
	}()

	cl.Stats(ctx, nmCfg.Name())
}
