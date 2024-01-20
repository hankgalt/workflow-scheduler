package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/comfforts/logger"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"

	lcadence "github.com/hankgalt/workflow-scheduler/pkg/clients/cadence"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	lprom "github.com/hankgalt/workflow-scheduler/pkg/clients/prometheus"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

const defaultConfigFile = "config/development.yaml"
const DEFAULT_WORKER_HOST = "business-worker"
const DEFAULT_METRICS_PORT = 9083

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

	cfgPath := os.Getenv("CADENCE_CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = defaultConfigFile
	}

	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "data"
	}

	credsPath := os.Getenv("CREDS_PATH")
	if credsPath == "" {
		l.Error(file.ERR_MISSING_CLOUD_CRED)
		panic(file.ErrMissingCloudCred)
	}

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(file.ERR_MISSING_CLOUD_BUCKET)
		panic(file.ErrMissingCloudBucket)
	}

	configData, err := os.ReadFile(cfgPath)
	if err != nil {
		l.Error("failed to read cadence config file", zap.Error(err), zap.String("cfgFile", cfgPath))
		panic(err)
	}

	var cadenceCfg lcadence.Configuration
	if err := yaml.Unmarshal(configData, &cadenceCfg); err != nil {
		l.Error("error initializing cadence configuration", zap.Error(err))
		panic(err)
	}

	port, err := strconv.Atoi(os.Getenv("METRICS_PORT"))
	if err != nil {
		port = DEFAULT_METRICS_PORT
	}
	addr := fmt.Sprintf(":%d", port)
	reporter, err := lprom.NewPrometheusReporter(addr, l)
	if err != nil {
		l.Error("error initializing prometheus reporter", zap.Error(err))
		panic(err)
	}

	svcMetricsScope := lprom.NewServiceScope(reporter)
	builder := lcadence.NewBuilder(l).
		SetHostPort(cadenceCfg.HostNameAndPort).
		SetDomain(cadenceCfg.DomainName).
		SetMetricsScope(svcMetricsScope)
	service, err := builder.BuildServiceClient()
	if err != nil {
		l.Error("error initializing cadence service client", zap.Error(err))
		panic(err)
	}

	wkMetricsScope := lprom.NewWorkerScope(reporter)

	cloudCfg, err := cloud.NewCloudConfig(credsPath, dataPath)
	if err != nil {
		l.Error(file.ERR_CLOUD_CFG_INIT, zap.Error(err))
		panic(err)
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, l)
	if err != nil {
		l.Error(file.ERR_CLOUD_CLIENT_INIT, zap.Error(err))
		panic(err)
	}

	schClOpts := scheduler.CalleeClientOptions(host)
	schClient, err := scheduler.NewClient(l, schClOpts)
	if err != nil {
		l.Error(common.ERR_SCH_CLIENT_INIT, zap.Error(err))
		panic(err)
	}

	bgCtx := context.WithValue(context.Background(), cloud.CloudClientContextKey, cloudClient)
	bgCtx = context.WithValue(bgCtx, scheduler.SchedulerClientContextKey, schClient)
	bgCtx = context.WithValue(bgCtx, cloud.CloudBucketContextKey, bucket)
	bgCtx = context.WithValue(bgCtx, file.DataPathContextKey, dataPath)

	workerOptions := worker.Options{
		MetricsScope:              wkMetricsScope,
		Logger:                    l,
		EnableLoggingInReplay:     true,
		EnableSessionWorker:       true,
		BackgroundActivityContext: bgCtx,
	}

	worker := worker.New(service, cadenceCfg.DomainName, bizwkfl.ApplicationName, workerOptions)
	registerBusinessWorkflow(worker)

	// Start worker
	if err := worker.Start(); err != nil {
		l.Error("error starting cadence business worker", zap.Error(err))
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
	worker.RegisterActivityWithOptions(common.CreateRunActivity, activity.RegisterOptions{Name: common.CreateRunActivityName})
	worker.RegisterActivityWithOptions(common.UpdateRunActivity, activity.RegisterOptions{Name: common.UpdateRunActivityName})
	worker.RegisterActivityWithOptions(common.SearchRunActivity, activity.RegisterOptions{Name: common.SearchRunActivityName})
	worker.RegisterActivityWithOptions(file.DownloadFileActivity, activity.RegisterOptions{Name: file.DownloadFileActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddAgentActivity, activity.RegisterOptions{Name: bizwkfl.AddAgentActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{Name: bizwkfl.AddPrincipalActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.AddFilingActivity, activity.RegisterOptions{Name: bizwkfl.AddFilingActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{Name: bizwkfl.GetCSVHeadersActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{Name: bizwkfl.GetCSVOffsetsActivityName})
	worker.RegisterActivityWithOptions(bizwkfl.ReadCSVActivity, activity.RegisterOptions{Name: bizwkfl.ReadCSVActivityName})
}
