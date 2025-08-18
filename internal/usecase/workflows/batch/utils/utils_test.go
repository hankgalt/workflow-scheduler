package utils_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
)

func TestBuildTransformerWithRules(t *testing.T) {
	inputs := [][][]string{
		{
			{"ENTITY_NAME", "ENTITY_NUM", "PHYSICAL_ADDRESS", "STATUS", "POSITION_TYPE"},
			{"Alice", "306749", "23781 WASHINGTON AVE C-100 #184 MURRIETA CA 92562 United States", "", "Manager"},
		},
		{
			{"ENTITY_NAME", "ENTITY_NUM", "PHYSICAL_ADDRESS1", "PHYSICAL_ADDRESS2", "PHYSICAL_ADDRESS3", "PHYSICAL_CITY", "PHYSICAL_STATE", "PHYSICAL_COUNTRY", "PHYSICAL_POSTAL_CODE", "STATUS", "POSITION_TYPE"},
			{"Alice", "306749", "23781 WASHINGTON AVE C-100 #184", "", "", "MURRIETA", "CA", "United States", "92562", "", "Manager"},
		},
	}

	rules := btchutils.BuildBusinessModelTransformRules()
	require.NotEmpty(t, rules, "Transform rules should not be empty")

	for _, input := range inputs {
		tranFunc := btchutils.BuildTransformerWithRules(input[0], rules)
		mapped := tranFunc(input[1])
		require.Equal(t, "306749", mapped["entity_id"])
		require.Equal(t, "Alice", mapped["entity_name"])
		require.Equal(t, "23781 WASHINGTON AVE C-100 #184 MURRIETA CA 92562 United States", mapped["address"])
		require.Equal(t, "", mapped["status"])
		require.Equal(t, "Manager", mapped["position_type"])
		require.Equal(t, "Principal", mapped["agent_type"])
	}
}
