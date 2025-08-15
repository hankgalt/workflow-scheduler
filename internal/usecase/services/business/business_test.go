package business_test

import (
	"context"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/business"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestBusinessEntitiesCRUD(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	l.Info("BusinessService - TestBusinessEntitiesCRUD initialized logger")

	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	svcCfg := business.NewBusinessServiceConfig(mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	bs, err := business.NewBusinessService(ctx, svcCfg)
	require.NoError(t, err)
	defer func() {
		err := bs.Close(ctx)
		require.NoError(t, err)
	}()

	entityId := "1234567"
	agM := getDummyAgent(entityId)

	agId, ag, err := bs.AddAgent(ctx, agM)
	require.NoError(t, err)
	require.Equal(t, ag.EntityID, entityId, "Expected agent entity ID to match")
	require.Equal(t, ag.EntityName, agM.EntityName, "Expected agent entity name to match")

	persistedAg, err := bs.GetAgent(ctx, agId)
	require.NoError(t, err)
	require.Equal(t, persistedAg.EntityID, ag.EntityID, "Expected agent entity ID to match")
	require.Equal(t, persistedAg.EntityName, ag.EntityName, "Expected agent entity name to match")

	resp, err := bs.DeleteEntity(ctx, stores.EntityTypeAgent, agId)
	require.NoError(t, err)
	require.Equal(t, resp, true, "Expected agent deletion to be successful")

	entityId = "7654321"
	fiM := getDummyFiling(entityId)
	fiId, fi, err := bs.AddFiling(ctx, fiM)
	require.NoError(t, err)
	require.Equal(t, fi.EntityID, entityId, "Expected filing entity ID to match")
	require.Equal(t, fi.EntityName, fiM.EntityName, "Expected filing entity name to match")

	persistedFi, err := bs.GetFiling(ctx, fiId)
	require.NoError(t, err)
	require.Equal(t, persistedFi.EntityID, fi.EntityID, "Expected filing entity ID to match")
	require.Equal(t, persistedFi.EntityName, fi.EntityName, "Expected filing entity name to match")

	resp, err = bs.DeleteEntity(ctx, stores.EntityTypeFiling, fiId)
	require.NoError(t, err)
	require.Equal(t, resp, true, "Expected filing deletion to be successful")
}

func getDummyAgent(entityId string) stores.Agent {
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

func getDummyFiling(entityId string) stores.Filing {
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
