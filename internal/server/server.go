package server

import (
	"context"
	"fmt"
	"log"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	// "github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

var _ api.SchedulerServer = (*grpcServer)(nil)

const (
	objectWildcard      = "*"
	createRunAction     = "create-run"
	updateRunAction     = "update-run"
	getRunAction        = "get-run"
	searchRunsAction    = "search-runs"
	deleteRunAction     = "delete-run"
	addEntityAction     = "add-entity"
	deleteEntityAction  = "delete-entity"
	getEntityAction     = "get-entity"
	fileSignalAction    = "file-signal"
	queryWorkflowAction = "query-workflow"
)

type subjectContextKey struct{}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	*zap.Logger
	Authorizer Authorizer
	// SchedulerService scheduler.SchedulerService
}

type grpcServer struct {
	api.SchedulerServer
	*Config
	NodeName string
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	hostname, err := os.Hostname()
	if err != nil {
		config.Error("error getting host name", zap.Error(err))
		return nil, fmt.Errorf("error getting host name: %w", err)
	}

	srv = &grpcServer{
		Config:   config,
		NodeName: hostname,
	}
	return srv, nil
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_auth.UnaryServerInterceptor(authenticate),
				grpc_auth.UnaryServerInterceptor(metadataLogger),
			),
		),
	)

	config.Info("creating gRPC server instance")
	gsrv := grpc.NewServer(opts...)
	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	config.Info("created biz gRPC server instance", zap.String("node-name", srv.NodeName))

	config.Info("registring biz gRPC server instance")
	api.RegisterSchedulerServer(gsrv, srv)

	config.Info("enabling reflection for shops api")
	reflection.Register(gsrv)

	config.Info("creating health gRPC server instance")
	hserv := health.NewServer()
	hserv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	config.Info("registring health gRPC server instance")
	healthpb.RegisterHealthServer(gsrv, hserv)

	return gsrv, nil
}

func metadataLogger(ctx context.Context) (context.Context, error) {
	_, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Println("missing metadata")
	}
	return ctx, nil
}

// LogRequest is a gRPC UnaryServerInterceptor that will log the API call to stdOut
func LogRequest(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {

	fmt.Printf("Request for : %s\n", info.FullMethod)
	// Last but super important, execute the handler so that the actualy gRPC request is also performed
	return handler(ctx, req)
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.PermissionDenied,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

type errorParams struct {
	errModel string
	err      error
	errTxt   string
	errCode  codes.Code
}
