package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

const TEST_DIR = "data"

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
		config *Config,
	){
		"test workflow CRUD, succeeds":            testWorkflowCRUD,
		"test unauthorized client checks succeed": testUnauthorizedClient,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nbClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nbClient, config)
		})

		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.SchedulerClient,
	nbClient api.SchedulerClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:65051")
	require.NoError(t, err)

	// grpc client
	newClient := func(target config.ConfigurationTarget) (*grpc.ClientConn, api.SchedulerClient, []grpc.DialOption) {
		// Client TLS config
		tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{Target: target})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		addr := lis.Addr().String()
		conn, err := grpc.Dial(addr, opts...)
		require.NoError(t, err)
		client = api.NewSchedulerClient(conn)
		return conn, client, opts
	}

	cc, client, _ := newClient(config.CLIENT)
	nbcc, nbClient, _ := newClient(config.NOBODY_CLIENT)

	l := logger.NewTestAppZapLogger(TEST_DIR)
	serviceCfg, err := scheduler.NewServiceConfig("localhost", "", "", "", "", true)
	require.NoError(t, err)

	ps, err := scheduler.NewSchedulerService(serviceCfg, l)
	require.NoError(t, err)

	authorizer, err := config.SetupAuthorizer(l)
	require.NoError(t, err)

	cfg = &Config{
		SchedulerService: ps,
		Authorizer:       authorizer,
		Logger:           l,
	}
	if fn != nil {
		fn(cfg)
	}

	// Server TLS config
	srvTLSConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.SERVER,
		Addr:   lis.Addr().String(),
	})
	require.NoError(t, err)
	srvCreds := credentials.NewTLS(srvTLSConfig)

	// grpc server
	server, err := NewGRPCServer(cfg, grpc.Creds(srvCreds))
	require.NoError(t, err)

	go func() {
		err := server.Serve(lis)
		require.NoError(t, err)
	}()

	client = api.NewSchedulerClient(cc)

	return client, nbClient, cfg, func() {
		server.Stop()
		err := cc.Close()
		require.NoError(t, err)

		err = nbcc.Close()
		require.NoError(t, err)

		err = ps.Close()
		require.NoError(t, err)

		err = os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testWorkflowCRUD(t *testing.T, client, nbClient api.SchedulerClient, config *Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-create-run@gmail.com"
	wfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.STARTED))

	wfRun, err = client.UpdateRun(ctx, &api.UpdateRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
		RunId:      wfRun.Run.RunId,
		Status:     string(models.UPLOADED),
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.UPLOADED))

	resp, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}

func testUnauthorizedClient(t *testing.T, client, nbClient api.SchedulerClient, config *Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-unauth-create-run@gmail.com"
	resp, err := nbClient.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.Error(t, err)
	assert.Equal(t, resp, (*api.RunResponse)(nil), "create run by an unauthorized client should fail")
}
