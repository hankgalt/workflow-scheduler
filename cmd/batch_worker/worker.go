package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

const DEFAULT_WORKER_HOST = "batch-worker"

func main() {
	fmt.Println("Starting batch worker - setting up logger instance")
	l := logger.GetSlogMultiLogger("data")

	host, err := os.Hostname()
	if err != nil {
		l.Error("error getting host name, using default", "error", err.Error())
		host = DEFAULT_WORKER_HOST
	} else {
		host = fmt.Sprintf("%s-%s", host, DEFAULT_WORKER_HOST)
	}

	namespace := os.Getenv("WORKFLOW_DOMAIN")
	if namespace == "" {
		l.Error(temporal.ERR_MISSING_NAMESPACE)
		panic(temporal.ErrMissingNamespace)
	}

	temporalHost := os.Getenv("TEMPORAL_HOST")
	if host == "" {
		l.Error(temporal.ERR_MISSING_HOST)
		panic(temporal.ErrMissingHost)
	}

	clientOpts := client.Options{
		Namespace: namespace,
		HostPort:  temporalHost,
	}
	tClient, err := client.Dial(clientOpts)
	if err != nil {
		l.Error("error connecting temporal server", "error", err.Error())
		panic(err)
	}
	defer tClient.Close()

	workerOptions := worker.Options{
		EnableLoggingInReplay:     true,
		EnableSessionWorker:       true,
		BackgroundActivityContext: context.Background(),
	}
	worker := worker.New(tClient, btchwkfl.ApplicationName, workerOptions)

	registerBatchWorkflow(worker)

	// Start worker
	if err := worker.Start(); err != nil {
		l.Error("error starting temporal batch worker", "error", err.Error())
		panic(err)
	}

	l.Info("Started batch worker, will wait for interrupt signal to gracefully shutdown the worker", "worker", btchwkfl.ApplicationName)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer func() {
		l.Info("batch worker stopped", "worker", btchwkfl.ApplicationName)
		cancel()
	}()
	<-ctx.Done()
	l.Info("batch worker exiting", "worker", btchwkfl.ApplicationName)
}

func registerBatchWorkflow(worker worker.Worker) {
	// register batch task processing workflows
	worker.RegisterWorkflow(btchwkfl.ProcessLocalCSV)
	worker.RegisterWorkflow(btchwkfl.ProcessLocalCSVMongo)

	// register batch task processing activities
	worker.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: btchwkfl.SetupLocalCSVBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.SetupCloudCSVBatch, activity.RegisterOptions{Name: btchwkfl.SetupCloudCSVBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVMongoBatch, activity.RegisterOptions{Name: btchwkfl.SetupLocalCSVMongoBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVBatchData, activity.RegisterOptions{Name: btchwkfl.HandleLocalCSVBatchDataActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleCloudCSVBatchData, activity.RegisterOptions{Name: btchwkfl.HandleCloudCSVBatchDataActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVMongoBatchData, activity.RegisterOptions{Name: btchwkfl.HandleLocalCSVMongoBatchDataActivity})
}
