package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	grpchandler "github.com/hankgalt/workflow-scheduler/internal/delivery/business/grpc_handler"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/business"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

const SERVICE_PORT = 65052
const DEFAULT_SERVICE_HOST = "business-service"

func main() {
	// Initialize logger
	l := logger.GetSlogMultiLogger("data")

	// Set up server port from environment variable or use default
	serverPort, err := strconv.Atoi(os.Getenv("SERVER_PORT"))
	if err != nil || serverPort == 0 {
		if err != nil {
			l.Debug("error parsing server port, using default", "error", err.Error())
		} else {
			l.Debug("no server port provided, using default server port")
		}
		serverPort = SERVICE_PORT
	}
	servicePort := fmt.Sprintf(":%d", serverPort)

	// Set up tcp socket listener on the specified port
	l.Info("opening a tcp socket address")
	listener, err := net.Listen("tcp", servicePort)
	if err != nil {
		l.Error("error opening a tcp socket address", "error", err.Error())
		panic(err)
	}

	// Build MongoDB for business service, using env vars
	l.Info("setting up business server config")
	mCfg := envutils.BuildMongoStoreConfig()
	svcCfg := business.NewBusinessServiceConfig(mCfg)

	// Initialize the authorizer for the business service
	l.Info("setting up business authorizer")
	authorizer, err := config.SetupAuthorizer()
	if err != nil {
		l.Error("error initializing business authorizer instance", "error", err.Error())
		panic(err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	startCtx = logger.WithLogger(startCtx, l)

	// Initialize the BusinessService instance
	bs, err := business.NewBusinessService(startCtx, svcCfg)
	if err != nil {
		l.Error("error initializing business service instance", "error", err.Error())
		panic(err)
	}

	// Create the gRPC server configuration
	l.Info("setting up business server configuration")
	servCfg := &grpchandler.Config{
		BusinessService: bs,
		Authorizer:      authorizer,
	}

	// Server TLS configuration & credentials
	l.Info("setting up business server TLS config")
	srvTLSConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.SERVER,
		Addr:   listener.Addr().String(),
	})
	if err != nil {
		l.Error("error setting up business server TLS config", "error", err.Error())
		panic(err)
	}
	srvCreds := credentials.NewTLS(srvTLSConfig)

	// Initialize the gRPC server with the configuration and TLS credentials
	l.Info("initializing grpc server instance")
	server, err := grpchandler.NewGRPCServer(servCfg, grpc.Creds(srvCreds))
	if err != nil {
		l.Error("error initializing grpc server", "error", err.Error())
		panic(err)
	}
	l.Info("business server initialized")

	// Start the gRPC server in a goroutine
	go func() {
		l.Info("business server will start listening for requests", "port", listener.Addr().String(), "service-info", server.GetServiceInfo())
		if err := server.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			l.Error("business server failed to start serving", "error", err.Error())
		}
	}()

	// Wait for an interrupt signal to gracefully shut down the server
	l.Info("waiting for interrupt signal to gracefully shutdown the business server")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// On stop signal, gracefully stop the server
	l.Info("received stop signal, gracefully stopping business server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		if err := listener.Close(); err != nil {
			l.Error("error closing business server network listener", "error", err.Error())
		}
		server.GracefulStop()
		cancel()
	}()

	if err := bs.Close(shutdownCtx); err != nil {
		l.Error("error shutting down business service", "error", err.Error())
	}

	<-shutdownCtx.Done()
	l.Info("business server stopped")
}
