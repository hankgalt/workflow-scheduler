package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/comfforts/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	// "github.com/hankgalt/workflow-scheduler/pkg/models"
)

const SERVICE_PORT = 65051
const SERVICE_DOMAIN = "127.0.0.1"

func main() {

	// initialize app logger instance
	logCfg := &logger.AppLoggerConfig{
		FilePath: filepath.Join("logs", "client.log"),
		Level:    zapcore.DebugLevel,
		Name:     "scheduler-client",
	}
	l := logger.NewAppZapLogger(logCfg)

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{Target: config.CLIENT})
	if err != nil {
		l.Fatal("error setting client TLS", zap.Error(err))
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	servicePort := fmt.Sprintf("%s:%d", SERVICE_DOMAIN, SERVICE_PORT)
	conn, err := grpc.Dial(servicePort, opts...)
	if err != nil {
		l.Fatal("client failed to connect", zap.Error(err))
	}
	defer conn.Close()

	client := api.NewSchedulerClient(conn)
	err = testWorkflowCRUD(client, l)
	if err != nil {
		l.Fatal("error: workflow CRUD", zap.Error(err))
	}
}

func testWorkflowCRUD(client api.SchedulerClient, l logger.AppLogger) error {
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
