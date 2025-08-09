package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
)

const DEFAULT_WORKER_HOST = "batch-worker"

func main() {
	fmt.Println("Starting batch worker - setting up logger instance")
	l := logger.GetSlogMultiLogger("data")

	host, err := os.Hostname()
	if err != nil {
		l.Debug("error getting host name, using default", "error", err.Error())
		host = DEFAULT_WORKER_HOST
	} else {
		host = fmt.Sprintf("%s-%s", host, DEFAULT_WORKER_HOST)
	}

	namespace := os.Getenv("WORKFLOW_DOMAIN")
	temporalHost := os.Getenv("TEMPORAL_HOST")

	startupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	startupCtx = logger.WithLogger(startupCtx, l)

	connBuilder := temporal.NewTemporalClientConnectionBuilder(namespace, temporalHost).WithMetrics(host, ":9464", "otel-collector:4317")
	clientOpts, shutdown, tracingInt, err := connBuilder.Build(startupCtx)
	if err != nil {
		l.Error("error building temporal client options", "error", err.Error())
		if shutdown != nil {
			sErr := shutdown(startupCtx)
			if sErr != nil {
				l.Error("error shutting down OTel", "error", sErr.Error())
				err = errors.Join(sErr, err)
			}
		}
		panic(fmt.Errorf("error building temporal client options: %w", err))
	}

	tClient, err := client.Dial(clientOpts)
	if err != nil {
		l.Error("error connecting temporal server", "error", err.Error())
		panic(err)
	}
	defer func() {
		l.Info("closing temporal client", "host", host)
		tClient.Close()
	}()

	workerOptions := worker.Options{
		BackgroundActivityContext: context.Background(),
		Interceptors:              []interceptor.WorkerInterceptor{tracingInt},
		EnableLoggingInReplay:     true,
		EnableSessionWorker:       true,
	}
	worker := worker.New(tClient, btchwkfl.ApplicationName, workerOptions)

	registerBatchWorkflow(worker)

	// Start worker
	if err := worker.Start(); err != nil {
		l.Error("error starting temporal batch worker", "error", err.Error())
		panic(err)
	}
	l.Info("Batch worker started, will wait for interrupt signal to gracefully shutdown the worker", "host", host, "worker", btchwkfl.ApplicationName)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tClient.Close()

	<-shutdownCtx.Done()
	l.Info("batch worker exiting", "host", host)
}

func registerBatchWorkflow(worker worker.Worker) {
	// register batch task processing workflows
	worker.RegisterWorkflowWithOptions(btchwkfl.ProcessLocalCSV, workflow.RegisterOptions{Name: btchwkfl.ProcessLocalCSVWorkflow})
	worker.RegisterWorkflowWithOptions(btchwkfl.ProcessLocalCSVMongo, workflow.RegisterOptions{Name: btchwkfl.ProcessLocalCSVMongoWorkflow})

	// register batch task processing activities
	worker.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVBatch, activity.RegisterOptions{Name: btchwkfl.SetupLocalCSVBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.SetupCloudCSVBatch, activity.RegisterOptions{Name: btchwkfl.SetupCloudCSVBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.SetupLocalCSVMongoBatch, activity.RegisterOptions{Name: btchwkfl.SetupLocalCSVMongoBatchActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVBatchData, activity.RegisterOptions{Name: btchwkfl.HandleLocalCSVBatchDataActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleCloudCSVBatchData, activity.RegisterOptions{Name: btchwkfl.HandleCloudCSVBatchDataActivity})
	worker.RegisterActivityWithOptions(btchwkfl.HandleLocalCSVMongoBatchData, activity.RegisterOptions{Name: btchwkfl.HandleLocalCSVMongoBatchDataActivity})
}
