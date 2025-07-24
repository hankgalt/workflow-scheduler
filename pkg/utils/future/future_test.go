package future_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/pkg/utils/future"
)

func TestDummyFuture(t *testing.T) {
	greet := func(name string) string {
		return "Hello, " + name
	}

	fut := future.NewDummyFuture(time.Second*2, greet, "world")
	require.False(t, fut.IsReady())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var result string
	err := fut.Get(ctx, fut.Arg(), &result)
	require.NoError(t, err)
	require.Equal(t, "Hello, "+fut.Arg(), result)

	require.True(t, fut.IsReady())
}
