package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

const SERVICE_PORT = 65051
const SERVICE_DOMAIN = "127.0.0.1"

func main() {

	// initialize app logger instance
	// l := logger.GetZapLogger("data", "scheduler-client")
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
}

func testWorkflowCRUD(client api.SchedulerClient, l logger.Logger) error {
	ctx := context.Background()

	requester := "test-create-run@gmail.com"
	wfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	if err != nil {
		l.Error("error creating run", zap.Error(err))
		return err
	}

	wfRun, err = client.UpdateRun(ctx, &api.UpdateRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
		RunId:      wfRun.Run.RunId,
		// Status:     string(models.UPLOADED),
	})
	if err != nil {
		l.Error("error updating run", zap.Error(err))
		return err
	}

	if _, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	}); err != nil {
		l.Error("error deleting run", zap.Error(err))
		return err
	}

	return nil
}
