package grpchandler_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
	grpchandler "github.com/hankgalt/workflow-scheduler/internal/delivery/scheduler/grpc_handler"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/scheduler"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

const TEST_DIR = "data"

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
	){
		"test unauthorized client checks succeed": testUnauthorizedClient,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nbClient, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nbClient)
		})

		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func TestWorkflowRuns(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
	){
		"test workflow run CRUD, succeeds": testWorkflowCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nbClient, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nbClient)
		})

		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func TestWorkflows(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
	){
		"test process cloud CSV to mongo workflow, started": testProcessCloudCSVMongoWorkflow,
		// "test process local CSV to mongo workflow, started": testProcessLocalCSVMongoWorkflow,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nbClient, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nbClient)
		})

		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testProcessCloudCSVMongoWorkflow(t *testing.T, client, nbClient api.SchedulerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	// Process cloud CSV to mongo workflow
	resp, err := client.ProcessCloudCSVMongoWorkflow(ctx, &api.BatchCSVRequest{
		MaxInProcessBatches: 2,
		BatchSize:           400,
		MappingRules:        map[string]*api.Rule{},
	})
	require.NoError(t, err)

	l.Info("ProcessCloudCSVMongoWorkflow - started workflow successfully", "workflow-run", resp)
}

func testProcessLocalCSVMongoWorkflow(t *testing.T, client, nbClient api.SchedulerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	// Process local CSV to mongo workflow
	resp, err := client.ProcessLocalCSVMongoWorkflow(ctx, &api.BatchCSVRequest{
		MaxInProcessBatches: 2,
		BatchSize:           400,
		MappingRules:        BuildBusinessModelTransformRules(),
	})
	require.NoError(t, err)

	l.Info("ProcessLocalCSVMongoWorkflow - started workflow successfully", "workflow-run", resp)
}

func testUnauthorizedClient(t *testing.T, client, nbClient api.SchedulerClient) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	_, err := nbClient.CreateRun(ctx, &api.RunRequest{
		WorkflowId: "S3r43r-T3s73k7l0w",
		RunId:      "S3r43r-T3s7Ru41d",
	})
	require.Error(t, err)

	stErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, stErr.Code())
	assert.Equal(t, grpchandler.ERR_UNAUTHORIZED_CREATE_RUN, stErr.Message())
}

func testWorkflowCRUD(t *testing.T, client, nbClient api.SchedulerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	wkflId, runId := "C3r43r-T3s73k7l0w", "C3r43r-T3s7Ru41d"
	_, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId: wkflId,
		RunId:      runId,
	})
	require.NoError(t, err)

	wfRun, err := client.GetRun(ctx, &api.RunRequest{
		RunId: runId,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.WorkflowId, wkflId)

	resp, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}

func setupTest(t *testing.T, fn func(*grpchandler.Config)) (
	client api.SchedulerClient,
	nbClient api.SchedulerClient,
	teardown func(),
) {
	t.Helper()

	l := logger.GetSlogLogger()

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

	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	tCfg := envutils.BuildTemporalConfig("GRPCHandlerTest")
	require.NotEmpty(t, tCfg.Host, "Temporal host should not be empty")

	svcCfg := scheduler.NewSchedulerServiceConfig(tCfg, mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ss, err := scheduler.NewSchedulerService(ctx, svcCfg)
	require.NoError(t, err)

	authorizer, err := config.SetupAuthorizer()
	require.NoError(t, err)

	cfg := &grpchandler.Config{
		SchedulerService: ss,
		Authorizer:       authorizer,
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
	server, err := grpchandler.NewGRPCServer(cfg, grpc.Creds(srvCreds))
	require.NoError(t, err)

	go func() {
		err := server.Serve(lis)
		require.NoError(t, err)
	}()

	client = api.NewSchedulerClient(cc)

	return client, nbClient, func() {
		err := cc.Close()
		require.NoError(t, err)

		err = nbcc.Close()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ctx = logger.WithLogger(ctx, l)

		err = ss.Close(ctx)
		require.NoError(t, err)

		server.GracefulStop()

		// err = os.RemoveAll(TEST_DIR)
		// require.NoError(t, err)
	}
}

func BuildBusinessModelTransformRules() map[string]*api.Rule {
	return map[string]*api.Rule{
		"ENTITY_NUM":                  {Target: "ENTITY_ID"},                                // rename to ENTITY_ID
		"FIRST_NAME":                  {Target: "NAME", Group: true, Order: 1},              // group into NAME
		"MIDDLE_NAME":                 {Target: "NAME", Group: true, Order: 2},              // group into NAME
		"LAST_NAME":                   {Target: "NAME", Group: true, Order: 3},              // group into NAME
		"PHYSICAL_ADDRESS":            {Target: "ADDRESS"},                                  // rename to ADDRESS
		"PHYSICAL_ADDRESS1":           {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"PHYSICAL_ADDRESS2":           {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"PHYSICAL_ADDRESS3":           {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"PHYSICAL_CITY":               {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"PHYSICAL_STATE":              {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"PHYSICAL_POSTAL_CODE":        {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"PHYSICAL_COUNTRY":            {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"ADDRESS1":                    {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"ADDRESS2":                    {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"ADDRESS3":                    {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"CITY":                        {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"STATE":                       {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"POSTAL_CODE":                 {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"COUNTRY":                     {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"PRINCIPAL_ADDRESS":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 1}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS1":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 2}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS2":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 3}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_CITY":              {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 4}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_STATE":             {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 5}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_POSTAL_CODE":       {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 6}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_COUNTRY":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 7}, // group into PRINCIPAL_ADDRESS
		"MAILING_ADDRESS":             {Target: "MAILING_ADDRESS", Group: true, Order: 1},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS1":            {Target: "MAILING_ADDRESS", Group: true, Order: 2},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS2":            {Target: "MAILING_ADDRESS", Group: true, Order: 3},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS3":            {Target: "MAILING_ADDRESS", Group: true, Order: 4},   // group into MAILING_ADDRESS
		"MAILING_CITY":                {Target: "MAILING_ADDRESS", Group: true, Order: 5},   // group into MAILING_ADDRESS
		"MAILING_STATE":               {Target: "MAILING_ADDRESS", Group: true, Order: 6},   // group into MAILING_ADDRESS
		"MAILING_POSTAL_CODE":         {Target: "MAILING_ADDRESS", Group: true, Order: 7},   // group into MAILING_ADDRESS
		"MAILING_COUNTRY":             {Target: "MAILING_ADDRESS", Group: true, Order: 8},   // group into MAILING_ADDRESS
		"PRINCIPAL_ADDRESS_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 1},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS1_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 2},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS2_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 3},     // group into ADDRESS_IN_CA
		"PRINCIPAL_CITY_IN_CA":        {Target: "ADDRESS_IN_CA", Group: true, Order: 4},     // group into ADDRESS_IN_CA
		"PRINCIPAL_STATE_IN_CA":       {Target: "ADDRESS_IN_CA", Group: true, Order: 5},     // group into ADDRESS_IN_CA
		"PRINCIPAL_POSTAL_CODE_IN_CA": {Target: "ADDRESS_IN_CA", Group: true, Order: 6},     // group into ADDRESS_IN_CA
		"PRINCIPAL_COUNTRY_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 7},     // group into ADDRESS_IN_CA
		"POSITION_TYPE":               {Target: "AGENT_TYPE", NewField: "Principal"},        // new Target field AGENT_TYPE with value Principal
	}
}
