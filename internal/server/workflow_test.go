package server_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

const DATA_PATH string = "scheduler"

func TestSchedulerServiceWorkflows(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
		config *server.Config,
	){
		// "process file signal workflow run, succeeds": testProcessFileSignalWorkflowRun,
		"query file signal workflow run, succeeds": testQueryWorkflowState,
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

func TestInt64Conversion(t *testing.T) {
	var i int64 = 1844674407370955161
	fmt.Printf("i: %d\n", i)
	fmt.Printf("i: %s\n", strconv.FormatInt(i, 10))
	fmt.Printf("i: %s\n", fmt.Sprintf("%d", i))
}

func testProcessFileSignalWorkflowRun(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqstr := "process-file-signal-workflow-server-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	wfRun, err := client.ProcessFileSignalWorkflow(ctx, &api.FileSignalRequest{
		FilePath:    filePath,
		Type:        api.EntityType_AGENT,
		RequestedBy: reqstr,
	})

	require.NoError(t, err)
	fmt.Println()
	fmt.Println("workflow run details: ", wfRun)
	fmt.Println()
}

func testQueryWorkflowState(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wfState, err := client.QueryWorkflowState(ctx, &api.QueryWorkflowRequest{
		RunId:      "80d4449b-a342-4741-84f3-5e4eb4c54d08",
		WorkflowId: "file-scheduler/Agents-sm.csv",
	})
	require.NoError(t, err)

	fields := wfState.GetState().GetFields()
	for k, v := range fields {
		printFieldValue(k, v, false)
	}
}

func printFieldValue(k string, v *structpb.Value, addTab bool) {
	switch v.Kind.(type) {
	case *structpb.Value_StringValue:
		if k != "" {
			fmt.Println()
			if addTab {
				fmt.Printf("\t StringValue - %s: %s", k, v.GetStringValue())
			} else {
				fmt.Printf("StringValue - %s: %s", k, v.GetStringValue())
			}
			fmt.Println()
		} else {
			fmt.Printf("%s ", v.GetStringValue())
		}
	case *structpb.Value_NumberValue:
		fmt.Println()
		fmt.Printf("NumberValue - %s: %f", k, v.GetNumberValue())
		fmt.Println()
	case *structpb.Value_BoolValue:
		fmt.Println()
		fmt.Printf("BoolValue - %s: %t", k, v.GetBoolValue())
		fmt.Println()
	case *structpb.Value_StructValue:
		fmt.Println()
		fmt.Printf("StructValue - %s: \n", k)
		for key, value := range v.GetStructValue().GetFields() {
			printFieldValue(key, value, true)
		}
		fmt.Println()
	case *structpb.Value_ListValue:
		fmt.Println()
		if addTab {
			fmt.Printf("\t ListValue - %s: [ ", k)
		} else {
			fmt.Printf("ListValue - %s: [ ", k)
		}

		fmt.Printf("ListValue - %s: [ ", k)
		for _, val := range v.GetListValue().Values {
			printFieldValue("", val, false)
		}
		fmt.Printf("]")
		fmt.Println()
	case *structpb.Value_NullValue:
		fmt.Println()
		fmt.Printf("NullValue - %s: %v", k, v.GetNullValue())
		fmt.Println()
	case interface{}:
		fmt.Println()
		fmt.Printf("interface - %s: %v", k, v)
		fmt.Println()
	default:
		fmt.Println()
		fmt.Printf("default - %s: %v", k, v)
		fmt.Println()
	}
}
