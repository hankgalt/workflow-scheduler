package grpchandler_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
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

	api "github.com/hankgalt/workflow-scheduler/api/business/v1"
	grpchandler "github.com/hankgalt/workflow-scheduler/internal/delivery/business/grpc_handler"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/business"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

const TEST_DIR = "data"

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.BusinessClient,
		nbClient api.BusinessClient,
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

func TestBusinessEntities(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.BusinessClient,
		nbClient api.BusinessClient,
	){
		"test Agent CRUD, succeeds":           testAgentCRUD,
		"test Agent Streaming CRUD, succeeds": testAddBusinessEntities,
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

func testUnauthorizedClient(t *testing.T, client, nbClient api.BusinessClient) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	// build id & entityId
	entity_id, entity_name := "535342788", "Zurn Concierge Nursing, Inc."

	// build Agent headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "AGENT_TYPE", "ADDRESS"}

	// build first record
	values := []string{entity_name, entity_id, "", "TTeri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	_, err := nbClient.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.Error(t, err)

	stErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, stErr.Code())
	assert.Equal(t, grpchandler.ERR_UNAUTHORIZED_ADD_ENTITY, stErr.Message())
}

func testAgentCRUD(t *testing.T, client, nbClient api.BusinessClient) {
	t.Helper()

	l := logger.GetSlogLogger()

	// build id & entityId
	entity_id, entity_name := "535342788", "Zurn Concierge Nursing, Inc."

	// build Agent headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "AGENT_TYPE", "ADDRESS"}

	// build first record
	values := []string{entity_name, entity_id, "", "TTeri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// add agent
	resp, err := client.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.NoError(t, err)

	// validate agent
	bp := resp.GetAgent()
	require.Equal(t, bp.EntityId, entity_id)
	require.Equal(t, bp.EntityName, entity_name)

	// get agent
	gResp, err := client.GetEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_AGENT,
		Id:   bp.Id,
	})
	require.NoError(t, err)
	require.Equal(t, gResp.GetAgent().Id, bp.Id)
	require.Equal(t, gResp.GetAgent().EntityName, entity_name)

	// delete agent
	dResp, err := client.DeleteEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_AGENT,
		Id:   bp.Id,
	})
	require.NoError(t, err)
	require.Equal(t, dResp.Ok, true)
}

func testAddBusinessEntities(t *testing.T, client, nbClient api.BusinessClient) {
	t.Helper()

	l := logger.GetSlogLogger()

	// build id & entityId
	entity_num := "5353427"

	// build Agent headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "AGENT_TYPE", "ADDRESS"}

	// build first record
	values := []string{"Zurn Concierge Nursing, Inc.", entity_num, "", "Teri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// get entity request stream
	entityReqStream, err := client.AddBusinessEntities(ctx)
	require.NoError(t, err)

	// send first record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.NoError(t, err)

	// make channels for response processor
	resCh := make(chan *api.StreamEntityResponse)
	errCh := make(chan error)
	doneCh := make(chan struct{})

	// start processing responses in background
	go asynClientBiDirectBusinessEntitiesProcessing(t, entityReqStream, resCh, errCh, doneCh)

	// setup aggregator waitgroup
	var wg sync.WaitGroup
	wg.Add(1)

	// processing results
	resIds := []string{}
	errs := []error{}

	l.Info("start aggregating results in background")
	go func() {
		for {
			select {
			case r, ok := <-resCh:
				if !ok {
					l.Info("result channel closed, closing waitgroup run & returning")
					wg.Done()
					return
				} else {
					if r.GetEntityResponse() != nil {
						// result aggregation
						bp := r.GetEntityResponse().GetAgent()
						require.Equal(t, bp.EntityId, entity_num)
						resIds = append(resIds, bp.Id)
					} else if r.GetError() != "" {
						// error aggregation
						l.Info("received error: ", "error", r.GetError())
						errs = append(errs, errors.New(r.GetError()))
					}
				}
			case err, ok := <-errCh:
				if !ok {
					l.Info("error channel closed, closing waitgroup run & returning")
					wg.Done()
					return
				} else {
					// error aggregation
					l.Info("received error: ", "error", err)
					errs = append(errs, err)
				}
			}
		}
	}()

	// build next record
	values = []string{"Zurn Concierge Nursing, Inc.", entity_num, "", "Nonda", "", "Bhusti", "Floor Manager", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields = map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	// send next record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.NoError(t, err)

	// build next record
	values = []string{"Zurn Concierge Nursing, Inc.", entity_num, "", "Dhumshum", "", "Shampa", "People Manager", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields = map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	// build duplicate record
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.NoError(t, err)

	// send duplicate record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	require.NoError(t, err)

	// close entity request stream
	err = entityReqStream.CloseSend()
	require.NoError(t, err)

	// wait for aggregator go routine to finish
	wg.Wait()

	// send done signal to processor go routine
	doneCh <- struct{}{}

	// list results
	l.Info("result ids: ", "ids", resIds)
	l.Info("errors: ", "errors", errs)

	// validate results
	require.Equal(t, 3, len(resIds))
	// require.Equal(t, 1, len(errs))

	// delete records
	for _, id := range resIds {
		err := deleteEntity(t, client, &api.EntityRequest{
			Id:   id,
			Type: api.EntityType_AGENT,
		})
		require.NoError(t, err)
	}

}

// example entity response processor
func asynClientBiDirectBusinessEntitiesProcessing(
	t *testing.T,
	entityStream api.Business_AddBusinessEntitiesClient,
	resCh chan *api.StreamEntityResponse,
	errCh chan error,
	doneCh chan struct{},
) {
	t.Helper()

	for {
		res, err := entityStream.Recv()
		if err == io.EOF {
			t.Log("result stream ended, closing response channels & response processing")
			close(resCh)
			close(errCh)
			break
		}

		if err != nil {
			t.Log("result stream error: ", err)
			errCh <- err
		} else {
			resCh <- res
		}
	}
	<-doneCh
}

func deleteEntity(t *testing.T, bCl api.BusinessClient, req *api.EntityRequest) error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := bCl.DeleteEntity(ctx, req)
	if err != nil {
		return err
	}
	require.Equal(t, true, resp.Ok)
	return nil
}

func setupTest(t *testing.T, fn func(*grpchandler.Config)) (
	client api.BusinessClient,
	nbClient api.BusinessClient,
	teardown func(),
) {
	t.Helper()

	l := logger.GetSlogLogger()

	lis, err := net.Listen("tcp", "127.0.0.1:65051")
	require.NoError(t, err)

	// grpc client
	newClient := func(target config.ConfigurationTarget) (*grpc.ClientConn, api.BusinessClient, []grpc.DialOption) {
		// Client TLS config
		tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{Target: target})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		addr := lis.Addr().String()
		conn, err := grpc.Dial(addr, opts...)
		require.NoError(t, err)
		client = api.NewBusinessClient(conn)
		return conn, client, opts
	}

	cc, client, _ := newClient(config.CLIENT)
	nbcc, nbClient, _ := newClient(config.NOBODY_CLIENT)

	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	svcCfg := business.NewBusinessServiceConfig(mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	bs, err := business.NewBusinessService(ctx, svcCfg)
	require.NoError(t, err)

	authorizer, err := config.SetupAuthorizer()
	require.NoError(t, err)

	cfg := &grpchandler.Config{
		BusinessService: bs,
		Authorizer:      authorizer,
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

	client = api.NewBusinessClient(cc)

	return client, nbClient, func() {
		err := cc.Close()
		require.NoError(t, err)

		err = nbcc.Close()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ctx = logger.WithLogger(ctx, l)

		err = bs.Close(ctx)
		require.NoError(t, err)

		server.GracefulStop()

		// err = os.RemoveAll(TEST_DIR)
		// require.NoError(t, err)
	}
}
