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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/file"
)

const DEFAULT_WORKER_HOST = "business-worker"

func main() {
	fmt.Println("  initializing app logger instance")
	l := logger.NewAppZapLogger(&logger.AppLoggerConfig{
		Level: zapcore.DebugLevel,
		Name:  "scheduler-business-worker",
	})

	host, err := os.Hostname()
	if err != nil {
		l.Error("error getting host name, using default", zap.Error(err))
		host = DEFAULT_WORKER_HOST
	} else {
		host = fmt.Sprintf("%s-%s", host, DEFAULT_WORKER_HOST)
	}

	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "data"
	}

	credsPath := os.Getenv("CREDS_PATH")
	if credsPath == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_CRED)
		panic(fiwkfl.ErrMissingCloudCred)
	}

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_BUCKET)
		panic(fiwkfl.ErrMissingCloudBucket)
	}

	cloudCfg, err := cloud.NewCloudConfig(credsPath, dataPath)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CFG_INIT, zap.Error(err))
		panic(err)
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, l)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CLIENT_INIT, zap.Error(err))
		panic(err)
	}

	schClOpts := scheduler.CalleeClientOptions(host)
	schClient, err := scheduler.NewClient(l, schClOpts)
	if err != nil {
		l.Error(comwkfl.ERR_SCH_CLIENT_INIT, zap.Error(err))
		panic(err)
	}

	bgCtx := context.WithValue(context.Background(), cloud.CloudClientContextKey, cloudClient)
	bgCtx = context.WithValue(bgCtx, scheduler.SchedulerClientContextKey, schClient)
	bgCtx = context.WithValue(bgCtx, cloud.CloudBucketContextKey, bucket)
	bgCtx = context.WithValue(bgCtx, fiwkfl.DataPathContextKey, dataPath)

	clientOptions := client.Options{
		Namespace: "scheduler-domain",
		HostPort:  "localhost:7233",
	}
	tClient, err := client.Dial(clientOptions)
	if err != nil {
		l.Error("error connecting with temporal server", zap.Error(err))
	}
	defer tClient.Close()

	workerOptions := worker.Options{
		EnableLoggingInReplay:     true,
		EnableSessionWorker:       true,
		BackgroundActivityContext: bgCtx,
	}
	worker := worker.New(tClient, bizwkfl.ApplicationName, workerOptions)

	registerBusinessWorkflow(worker)

	// Start worker
	if err := worker.Start(); err != nil {
		l.Error("error starting temporal business worker", zap.Error(err))
		panic(err)
	}

	l.Info("Started business worker, will wait for interrupt signal to gracefully shutdown the worker", zap.String("worker", bizwkfl.ApplicationName))
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer func() {
		l.Info("business worker stopped", zap.String("worker", bizwkfl.ApplicationName))
		cancel()
	}()
	<-ctx.Done()
	l.Info("business worker exiting", zap.String("worker", bizwkfl.ApplicationName))
}

func registerBusinessWorkflow(worker worker.Worker) {
	// register business task processing workflows
	worker.RegisterWorkflow(bizwkfl.ProcessCSVWorkflow)
	worker.RegisterWorkflow(bizwkfl.ReadCSVWorkflow)
	worker.RegisterWorkflow(bizwkfl.ReadCSVRecordsWorkflow)
	worker.RegisterWorkflow(bizwkfl.ProcessFileSignalWorkflow)

	// register business task processing activities
	worker.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{Name: comwkfl.CreateRunActivityName})
	worker.RegisterActivityWithOptions(comwkfl.UpdateRunActivity, activity.RegisterOptions{Name: comwkfl.UpdateRunActivityName})
	worker.RegisterActivityWithOptions(comwkfl.SearchRunActivity, activity.RegisterOptions{Name: comwkfl.SearchRunActivityName})
	worker.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{Name: fiwkfl.DownloadFileActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddAgentActivity, activity.RegisterOptions{Name: bizwkfl.AddAgentActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{Name: bizwkfl.AddPrincipalActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddFilingActivity, activity.RegisterOptions{Name: bizwkfl.AddFilingActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: bizwkfl.GetCSVHeadersActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: bizwkfl.GetCSVOffsetsActivityName})
}
