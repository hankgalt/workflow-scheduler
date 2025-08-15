package vypar_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/repo/vypar"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestVyparRepoAgentCRUD(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	t.Log("TestVyparRepo Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// Get MongoDB configuration
	nmCfg := envutils.BuildMongoStoreConfig()
	ms, err := mongostore.NewMongoStore(ctx, nmCfg)
	require.NoError(t, err)

	defer func() {
		err := ms.Close(ctx)
		require.NoError(t, err)
	}()

	// Initialize Vypar repository
	vr, err := vypar.NewVyparRepo(ctx, ms)
	require.NoError(t, err)

	entityId := "1234567"
	agM := getDummyBusinessAgentMongo(entityId)

	t.Log("Vypar repository initialized successfully")
	// Test adding an agent
	agentId, err := vr.AddAgent(ctx, agM)
	require.NoError(t, err)
	require.NotEmpty(t, agentId, "Agent ID should not be empty")
	t.Logf("Agent added successfully with ID: %s", agentId)

	_, err = vr.AddAgent(ctx, agM)
	require.Error(t, err)
	require.Equal(t, vypar.ErrDuplicateAgent, err, "Expected duplicate agent error")

	// Test item count
	count, err := vr.GetItemCount(ctx, vypar.AGENT_COLLECTION)
	require.NoError(t, err)
	require.Greater(t, count, int64(0), "Item count should be greater than zero")
	t.Logf("Item count in collection '%s': %d", vypar.AGENT_COLLECTION, count)

	// Test getting agent by ID
	agent, err := vr.GetAgent(ctx, entityId)
	require.NoError(t, err)
	require.NotNil(t, agent, "Agent should not be nil")
	require.Equal(t, entityId, agent.EntityID, "Agent entity ID should match")
	t.Logf("Agent fetched successfully: %+v", agent)

	// Test deleting agent
	deleted, err := vr.DeleteAgent(ctx, entityId)
	require.NoError(t, err)
	require.True(t, deleted, "Agent should be deleted successfully")

	// Verify agent deletion
	_, err = vr.GetAgent(ctx, entityId)
	require.Error(t, err, "Expected error when fetching deleted agent")
	t.Logf("get agent error: %v", err)

	// Cleanup: Ensure no agents remain in the collection
	remainingCount, err := vr.GetItemCount(ctx, vypar.AGENT_COLLECTION)
	require.NoError(t, err)
	require.Equal(t, int64(0), remainingCount, "No agents should remain in the collection after tests")
	t.Log("All agents cleaned up successfully")
}

func TestYvparRepoFilingCRUD(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	t.Log("TestVyparRepo Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// Get MongoDB configuration
	nmCfg := envutils.BuildMongoStoreConfig()
	ms, err := mongostore.NewMongoStore(ctx, nmCfg)
	require.NoError(t, err)

	defer func() {
		err := ms.Close(ctx)
		require.NoError(t, err)
	}()

	// Initialize Vypar repository
	vr, err := vypar.NewVyparRepo(ctx, ms)
	require.NoError(t, err)

	entityId := "1234567"
	bfM := getDummyBusinessFilingMongo(entityId)

	t.Log("Vypar repository initialized successfully")
	// Test adding a filing
	filingId, err := vr.AddFiling(ctx, bfM)
	require.NoError(t, err)
	require.NotEmpty(t, filingId, "Filing ID should not be empty")
	t.Logf("Filing added successfully with ID: %s", filingId)

	// Test item count
	count, err := vr.GetItemCount(ctx, vypar.FILING_COLLECTION)
	require.NoError(t, err)
	require.Greater(t, count, int64(0), "Item count should be greater than zero")
	t.Logf("Item count in collection '%s': %d", vypar.FILING_COLLECTION, count)

	// Test getting filing by entity ID
	filing, err := vr.GetFiling(ctx, entityId)
	require.NoError(t, err)
	require.NotNil(t, filing, "Filing should not be nil")
	require.Equal(t, entityId, filing.EntityID, "Filing entity ID should match")
	t.Logf("Filing fetched successfully: %+v", filing)

	// Test deleting filing
	deleted, err := vr.DeleteFiling(ctx, entityId)
	require.NoError(t, err)
	require.True(t, deleted, "Filing should be deleted successfully")

	// Verify filing deletion
	_, err = vr.GetFiling(ctx, entityId)
	require.Error(t, err, "Expected error when fetching deleted filing")
	t.Logf("get filing error: %v", err)

	// Cleanup: Ensure no filings remain in the collection
	remainingCount, err := vr.GetItemCount(ctx, vypar.FILING_COLLECTION)
	require.NoError(t, err)
	require.Equal(t, int64(0), remainingCount, "No filings should remain in the collection after tests")
	t.Log("All filings cleaned up successfully")
}

func getDummyBusinessAgentMongo(entityId string) stores.Agent {
	return stores.Agent{
		EntityID:   entityId,
		EntityName: "TestEntity",
		OrgName:    "TestOrg",
		FirstName:  "John",
		MiddleName: "M",
		LastName:   "Doe",
		Address:    "123 Test St, Test City, TC 12345",
		AgentType:  "Individual",
	}
}

func getDummyBusinessFilingMongo(entityId string) stores.Filing {
	return stores.Filing{
		EntityID:               entityId,
		EntityName:             "TestEntity",
		InitialFilingDate:      uint64(time.Now().UnixNano()),
		Jurisdiction:           "TestJurisdiction",
		EntityStatus:           "Active",
		EntityType:             "Corporation",
		StandingSOS:            "Good Standing",
		FilingType:             "Annual Report",
		ForeignName:            "Test Foreign Name",
		StandingFTB:            "Good Standing",
		StandingVCFCF:          "Good Standing",
		SuspensionDate:         0, // No suspension
		LastSIFileNumber:       "",
		LastSIFileDate:         0, // No last SI file date
		PrincipalAddress:       "123 Test St, Test City, TC 12345",
		MailingAddress:         "123 Test St, Test City, TC 12345",
		PrincipalAddressInCA:   "123 Test St, Test City, TC 12345",
		LLCManagementStructure: "Member-managed",
		TypeOfBusiness:         "Test Business Type",
	}
}
