package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
)

const SERVICE_PORT = 65051
const SERVICE_DOMAIN = "127.0.0.1"

var agentHeaderMapping = map[string]string{
	"ENTITY_NUM":       "ENTITY_ID",
	"PHYSICAL_ADDRESS": "ADDRESS",
}

func main() {

	// initialize app logger instance
	l := logger.GetSlogLogger()

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{Target: config.CLIENT})
	if err != nil {
		l.Error("error setting client TLS", "error", err.Error())
		panic(err)
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	servicePort := fmt.Sprintf("%s:%d", SERVICE_DOMAIN, SERVICE_PORT)
	conn, err := grpc.Dial(servicePort, opts...)
	if err != nil {
		l.Error("client failed to connect", "error", err.Error())
		panic(err)
	}
	defer conn.Close()

	client := api.NewSchedulerClient(conn)
	err = testWorkflowCRUD(client, l)
	if err != nil {
		l.Error("error: workflow CRUD", "error", err.Error())
		return
	}

	err = testProcessCloudCSVMongoWorkflow(client, l)
	if err != nil {
		l.Error("error: process cloud CSV Mongo workflow", "error", err.Error())
		return
	}
}

func testWorkflowCRUD(client api.SchedulerClient, l logger.Logger) error {
	ctx := context.Background()

	wfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId: "S3r43r-T3s73k7l0w",
		RunId:      "S3r43r-T3s7Ru41d",
	})
	if err != nil {
		l.Error("error creating run", "error", err.Error())
		return err
	}

	wfRun, err = client.GetRun(ctx, &api.RunRequest{
		RunId: wfRun.Run.RunId,
	})
	if err != nil {
		l.Error("error fetching run", "error", err.Error())
		return err
	}

	if _, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	}); err != nil {
		l.Error("error deleting run", "error", err.Error())
		return err
	}

	return nil
}

func testProcessCloudCSVMongoWorkflow(client api.SchedulerClient, l logger.Logger) error {
	ctx := context.Background()

	resp, err := client.ProcessCloudCSVMongoWorkflow(ctx, &api.BatchCSVRequest{
		MaxBatches:   2,
		BatchSize:    400,
		MappingRules: map[string]*api.Rule{},
	})
	if err != nil {
		l.Error("error processing cloud CSV to mongo workflow", "error", err.Error())
		return err
	}

	l.Info("ProcessCloudCSVMongoWorkflow response", "response", resp)

	return nil
}
