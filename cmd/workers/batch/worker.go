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
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"
	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	bsinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/snapshotters"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

const DEFAULT_WORKER_HOST = "batch-worker"

func main() {
	fmt.Println("Starting batch worker - setting up logger instance")
	l := logger.GetSlogMultiLogger("data")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// setup host identity for worker
	host, err := os.Hostname()
	if err != nil {
		l.Debug("error getting host name, using default", "error", err.Error())
		host = DEFAULT_WORKER_HOST
	} else {
		host = fmt.Sprintf("%s-%s", host, DEFAULT_WORKER_HOST)
	}

	// build temporal client config from environment variables
	tCfg := envutils.BuildTemporalConfig(host)

	// build temporal client connection options
	connBuilder := temporal.NewTemporalClientConnectionBuilder(
		tCfg.Namespace(),
		tCfg.Host(),
	).WithMetrics(
		tCfg.ClientName(),
		tCfg.MetricsAddr(),
		tCfg.OtelEndpoint(),
	)

	// setup startup context with timeout
	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	startupCtx = logger.WithLogger(startupCtx, l)

	clientOpts, shutdown, tracingInt, err := connBuilder.Build(startupCtx)
	defer func() {
		if shutdown != nil {
			// shutdown OTel if it was initialized
			l.Info("closing otel client", "host", host)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := shutdown(ctx); err != nil {
				l.Error("error shutting down OTel", "error", err.Error())
			} else {
				l.Info("OTel shutdown successfully")
			}
		}
	}()
	if err != nil {
		l.Error("error building temporal client options", "error", err.Error())
		panic(fmt.Errorf("error building temporal client options: %w", err))
	}

	// get temporal client instance
	tClient, err := client.Dial(clientOpts)
	if err != nil {
		l.Error("error connecting temporal server", "error", err.Error())
		panic(err)
	}
	defer func() {
		l.Info("closing temporal client", "host", host)
		tClient.Close()
	}()

	// build worker options with tracing interceptor & initialize worker instance
	// TODO - add worker context
	workerOptions := worker.Options{
		BackgroundActivityContext: ctx,
		Interceptors:              []interceptor.WorkerInterceptor{tracingInt},
		EnableLoggingInReplay:     true,
		EnableSessionWorker:       true,
	}
	worker := worker.New(tClient, btchwkfl.ApplicationName, workerOptions)
	defer func() {
		l.Info("closing temporal worker", "host", host)
		worker.Stop()
	}()

	// register workflows and activities
	registerBatchWorkflow(worker)

	// start worker
	if err := worker.Start(); err != nil {
		l.Error("error starting temporal batch worker", "error", err.Error())
		panic(err)
	}
	l.Info(
		"Batch worker started, will wait for interrupt signal to gracefully shutdown the worker",
		"host", host,
		"worker", btchwkfl.ApplicationName,
	)

	// wait for interrupt signal to gracefully shutdown the worker
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// setup shutdown context with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	<-shutdownCtx.Done()
	l.Info("batch worker exiting", "host", host)
}

func registerBatchWorkflow(worker worker.Worker) {
	// register batch task processing workflows
	// Register the workflow
	worker.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, bsinks.MongoSinkConfig[domain.CSVRow], snapshotters.LocalSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias,
		},
	)
	worker.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.CloudCSVConfig, bsinks.MongoSinkConfig[domain.CSVRow], snapshotters.CloudSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias,
		},
	)

	// register batch task processing activities
	worker.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	worker.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
		},
	)
	worker.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)
	worker.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, bsinks.NoopSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextNoopSinkBatchActivityAlias,
		},
	)
	worker.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotLocalBatchActivityAlias,
		},
	)
	worker.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.CloudSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotCloudBatchActivityAlias,
		},
	)
}
