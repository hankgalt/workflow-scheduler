package server

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/comfforts/errors"
)

func TestEntities(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
		config *Config,
	){
		"test Principal CRUD, succeeds":           testPrincipalCRUD,
		"test Principal Streaming CRUD, succeeds": testAddBusinessEntities,
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

func testAddBusinessEntities(t *testing.T, client, nbClient api.SchedulerClient, config *Config) {
	t.Helper()

	// build id & entityId
	id := "5353427"
	num, err := strconv.Atoi(id)
	require.NoError(t, err)

	// build Principal headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "POSITION_TYPE", "ADDRESS"}

	// build first record
	values := []string{"Zurn Concierge Nursing, Inc.", id, "", "Teri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// get entity request stream
	entityReqStream, err := client.AddBusinessEntities(ctx)
	require.NoError(t, err)

	// send first record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// make channels for response processor
	resCh := make(chan *api.StreamAddEntityResponse)
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

	t.Log("start aggregating results in background")
	go func() {
		for {
			select {
			case r, ok := <-resCh:
				if !ok {
					t.Log("result channel closed, closing waitgroup run & returning")
					wg.Done()
					return
				} else {
					if r.GetEntityResponse() != nil {
						// result aggregation
						bp := r.GetEntityResponse().GetPrincipal()
						require.Equal(t, int(bp.EntityId), num)
						resIds = append(resIds, bp.Id)
					} else if r.GetError() != nil {
						// error aggregation
						t.Log("received error: ", r.GetError())
						errs = append(errs, errors.NewAppError(r.GetError().GetMessage()))
					}
				}
			case err, ok := <-errCh:
				if !ok {
					t.Log("error channel closed, closing waitgroup run & returning")
					wg.Done()
					return
				} else {
					// error aggregation
					t.Log("received error: ", err)
					errs = append(errs, err)
				}
			}
		}
	}()

	// build next record
	values = []string{"Zurn Concierge Nursing, Inc.", id, "", "Nonda", "", "Bhusti", "Floor Manager", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields = map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	// send next record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// build next record
	values = []string{"Zurn Concierge Nursing, Inc.", id, "", "Dhumshum", "", "Shampa", "People Manager", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields = map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	// build duplicate record
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// send duplicate record request
	err = entityReqStream.Send(&api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// close entity request stream
	err = entityReqStream.CloseSend()
	require.NoError(t, err)

	// wait for aggregator go routine to finish
	wg.Wait()

	// send done signal to processor go routine
	doneCh <- struct{}{}

	// list rrsults
	t.Log("result ids: ", resIds)
	t.Log("errors: ", errs)

	// validate results
	require.Equal(t, 3, len(resIds))
	require.Equal(t, 1, len(errs))

	// delete records
	for _, id := range resIds {
		err := deleteEntity(t, client, &api.DeleteEntityRequest{
			Id:   id,
			Type: api.EntityType_PRINCIPAL,
		})
		require.NoError(t, err)
	}
}

// example entity response processor
func asynClientBiDirectBusinessEntitiesProcessing(
	t *testing.T,
	entityStream api.Scheduler_AddBusinessEntitiesClient,
	resCh chan *api.StreamAddEntityResponse,
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

func testPrincipalCRUD(t *testing.T, client, nbClient api.SchedulerClient, config *Config) {
	t.Helper()

	// build id & entityId
	id := "5353427"
	num, err := strconv.Atoi(id)
	require.NoError(t, err)

	// build Principal headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "POSITION_TYPE", "ADDRESS"}

	// build first record
	values := []string{"Zurn Concierge Nursing, Inc.", id, "", "Teri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// add principal
	resp, err := client.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// validate principal
	bp := resp.GetPrincipal()
	require.Equal(t, bp.Id, id)
	require.Equal(t, int(bp.EntityId), num)

	// build next record
	values = []string{"Zurn Concierge Nursing, Inc.", id, "", "Nonda", "", "Bhusti", "Floor Manager", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields = map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	// add next principal
	resp, err = client.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_PRINCIPAL,
	})
	require.NoError(t, err)

	// validate next principal
	bp2 := resp.GetPrincipal()
	require.Equal(t, int(bp2.EntityId), num)

	// validate principal id uniqueness
	require.NotEqual(t, bp.Id, bp2.Id)

	// delete records
	for _, id := range []string{bp.Id, bp2.Id} {
		dResp, err := client.DeleteEntity(ctx, &api.DeleteEntityRequest{
			Type: api.EntityType_PRINCIPAL,
			Id:   id,
		})
		require.NoError(t, err)
		require.Equal(t, dResp.Ok, true)
	}
}

func deleteEntity(t *testing.T, bCl api.SchedulerClient, req *api.DeleteEntityRequest) error {
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
