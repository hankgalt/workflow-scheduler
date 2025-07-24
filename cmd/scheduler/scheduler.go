package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/server"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

const SERVICE_PORT = 65051

func main() {
	ctx := context.Background()

	fmt.Println("  initializing app logger instance")
	logCfg := &logger.AppLoggerConfig{
		Level: zapcore.DebugLevel,
		Name:  "scheduler",
	}
	l := logger.NewAppZapLogger(logCfg)

	serverPort, err := strconv.Atoi(os.Getenv("SERVER_PORT"))
	if err != nil || serverPort == 0 {
		l.Error("no server port provided, using default server port", zap.Error(err))
		serverPort = SERVICE_PORT
	}

	l.Info("opening a tcp socket address")
	servicePort := fmt.Sprintf(":%d", serverPort)
	listener, err := net.Listen("tcp", servicePort)
	if err != nil {
		l.Error("error opening a tcp socket address", zap.Error(err))
		panic(err)
	}

	l.Info("setting up scheduler service config")
	serviceCfg, err := scheduler.NewServiceConfig("", "", "", "", true)
	if err != nil {
		l.Error("error creating scheduler service config", zap.Error(err))
		panic(err)
	}

	l.Info("setting up scheduler service")
	schServ, err := scheduler.NewSchedulerService(serviceCfg, l)
	if err != nil {
		l.Error("error creating scheduler service", zap.Error(err))
		panic(err)
	}

	l.Info("setting up scheduler authorizer")
	authorizer, err := config.SetupAuthorizer(l)
	if err != nil {
		l.Error("error initializing scheduler authorizer instance", zap.Error(err))
		panic(err)
	}

	servCfg := &server.Config{
		SchedulerService: schServ,
		Authorizer:       authorizer,
		Logger:           l,
	}

	l.Info("setting up scheduler server TLS config")
	srvTLSConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.SERVER,
		Addr:   listener.Addr().String(),
	})
	if err != nil {
		l.Error("error setting up scheduler server TLS config", zap.Error(err))
		panic(err)
	}
	srvCreds := credentials.NewTLS(srvTLSConfig)

	l.Info("initializing grpc server instance")
	server, err := server.NewGRPCServer(servCfg, grpc.Creds(srvCreds))
	if err != nil {
		l.Error("error initializing grpc server", zap.Error(err))
		panic(err)
	}
	l.Info("scheduler server initialized")

	go func() {
		l.Info("scheduler server will start listening for requests", zap.String("port", listener.Addr().String()), zap.Any("service-info", server.GetServiceInfo()))
		if err := server.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			l.Error("scheduler server failed to start serving", zap.Error(err), zap.Any("errorType", reflect.TypeOf(err)))
		}
	}()

	l.Info("waiting for interrupt signal to gracefully shutdown the scheduler server")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer func() {
		if err := listener.Close(); err != nil {
			l.Error("error closing scheduler server network listener", zap.Error(err), zap.Any("errorType", reflect.TypeOf(err)))
		}
		l.Info("stopping scheduler server")
		server.GracefulStop()
		cancel()
	}()
	<-ctx.Done()
	l.Info("scheduler server exiting")
}
