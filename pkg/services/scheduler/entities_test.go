package scheduler_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
	"github.com/stretchr/testify/require"
)

func TestAgentCRUD(t *testing.T) {
	setupEnv(t)

	logger := logger.NewTestAppZapLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		bs scheduler.SchedulerService,
	){
		"business agent CRUD, succeeds":     testAgentCRUD,
		"business principal CRUD, succeeds": testPrincipalCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			bs, teardown := setupAPITests(t, logger)
			defer teardown()
			fn(t, bs)
		})
	}

	err := os.RemoveAll(TEST_DIR)
	require.NoError(t, err)
}

func testAgentCRUD(t *testing.T, bs scheduler.SchedulerService) {
	t.Helper()

	id := "1234567"
	num, err := strconv.Atoi(id)
	require.NoError(t, err)

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	agent1, err := bs.AddAgent(ctx, &models.BusinessAgent{
		ID:              id,
		EntityID:        uint64(num),
		EntityName:      "T3s7Ru41d",
		OrgName:         "org",
		FirstName:       "John",
		MiddleName:      "M",
		LastName:        "Doe",
		PhysicalAddress: "123 John St Minpota FG 34526 MF",
		AgentType:       "Individual",
	})
	require.NoError(t, err)
	require.Equal(t, int(agent1.EntityID), num)

	agent2, err := bs.AddAgent(ctx, &models.BusinessAgent{
		ID:              id,
		EntityID:        uint64(num),
		EntityName:      "T3s7Ru41d",
		OrgName:         "org",
		FirstName:       "Menta",
		MiddleName:      "M",
		LastName:        "Doe",
		PhysicalAddress: "123 John St Minpota FG 34526 MF",
		AgentType:       "Individual",
	})
	require.NoError(t, err)
	require.Equal(t, int(agent2.EntityID), num)

	agent, err := bs.GetAgent(ctx, agent1.ID)
	require.NoError(t, err)
	require.Equal(t, agent.ID, agent1.ID)

	err = bs.DeleteAgent(ctx, agent1.ID)
	require.NoError(t, err)

	err = bs.DeleteAgent(ctx, agent2.ID)
	require.NoError(t, err)
}

func testPrincipalCRUD(t *testing.T, bs scheduler.SchedulerService) {
	t.Helper()

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	id := "1234567"
	num, err := strconv.Atoi(id)
	require.NoError(t, err)

	p1, err := bs.AddPrincipal(ctx, &models.BusinessPrincipal{
		ID:           id,
		EntityID:     uint64(num),
		EntityName:   "T3s7Ru41d",
		OrgName:      "org",
		FirstName:    "John",
		MiddleName:   "M",
		LastName:     "Doe",
		Address:      "123 John St Minpota FG 34526 MF",
		PositionType: "Manager",
	})
	require.NoError(t, err)
	require.Equal(t, int(p1.EntityID), num)

	pri, err := bs.GetPrincipal(ctx, p1.ID)
	require.NoError(t, err)
	require.Equal(t, pri.ID, p1.ID)

	defer func() {
		err = bs.DeletePrincipal(ctx, p1.ID)
		require.NoError(t, err)
	}()

	p2, err := bs.AddPrincipal(ctx, &models.BusinessPrincipal{
		ID:           id,
		EntityName:   "T3s7Ru41d",
		EntityID:     uint64(num),
		OrgName:      "org",
		FirstName:    "Menta",
		MiddleName:   "M",
		LastName:     "Doe",
		Address:      "123 John St Minpota FG 34526 MF",
		PositionType: "Individual",
	})
	require.NoError(t, err)
	require.Equal(t, int(p2.EntityID), num)

	defer func() {
		err = bs.DeletePrincipal(ctx, p2.ID)
		require.NoError(t, err)
	}()
}
