package grpchandler

import (
	"context"
	"fmt"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/scheduler/v1"
	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/scheduler"
)

var _ api.SchedulerServer = (*grpcServer)(nil)

const (
	ERR_UNAUTHORIZED_CREATE_RUN      = "unauthorized to create run"
	ERR_UNAUTHORIZED_GET_RUN         = "unauthorized to get run"
	ERR_UNAUTHORIZED_DELETE_RUN      = "unauthorized to delete run"
	ERR_UNAUTHORIZED_LOCAL_CSV_MONGO = "unauthorized to process local csv to mongo workflow"
	ERR_UNAUTHORIZED_CLOUD_CSV_MONGO = "unauthorized to process cloud csv to mongo workflow"
)

const (
	objectWildcard      = "*"
	createRunAction     = "create-run"
	getRunAction        = "get-run"
	deleteRunAction     = "delete-run"
	localCSVMongoAction = "local-csv-mongo"
	cloudCSVMongoAction = "cloud-csv-mongo"
)

type subjectContextKey struct{}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	Authorizer Authorizer
	scheduler.SchedulerService
}

type grpcServer struct {
	*Config
	NodeName string
	api.SchedulerServer
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	hostname, err := os.Hostname()
	if err != nil {
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
				grpc_auth.StreamServerInterceptor(decorateContext),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_auth.UnaryServerInterceptor(authenticate),
				grpc_auth.UnaryServerInterceptor(decorateContext),
				grpc_auth.UnaryServerInterceptor(metadataLogger),
			),
		),
	)

	gsrv := grpc.NewServer(opts...)
	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterSchedulerServer(gsrv, srv)

	reflection.Register(gsrv)

	hserv := health.NewServer()
	hserv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(gsrv, hserv)

	return gsrv, nil
}

// Workflows

func (s *grpcServer) ProcessLocalCSVMongoWorkflow(ctx context.Context, req *api.BatchCSVRequest) (*api.RunResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		localCSVMongoAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_LOCAL_CSV_MONGO)
		return nil, st.Err()
	}

	jobCfg := batch.MapLocalCSVMongoBatchConfigFromProto(req.GetLocalCsvMongoConfig())

	batReq := batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches:          uint(req.MaxBatches),
				MaxInProcessBatches: uint(req.MaxInProcessBatches),
				BatchSize:           uint(req.BatchSize),
				Start:               req.Start,
				MappingRules:        batch.MapRulesFromProto(req.MappingRules),
			},
		},
		Config: jobCfg,
	}

	resp, err := s.SchedulerService.ProcessLocalCSVToMongoWorkflow(ctx, batReq)
	if err != nil {
		l.Error("error processing local csv to mongo workflow", "error", err.Error(), "req", req)
		st := status.New(codes.Internal, err.Error())
		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	l.Debug("created local csv to mongo workflow run", "resp", resp)
	return &api.RunResponse{
		Ok:  true,
		Run: stores.MapWorkflowRunToProto(resp),
	}, nil
}

func (s *grpcServer) ProcessCloudCSVMongoWorkflow(ctx context.Context, req *api.BatchCSVRequest) (*api.RunResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		cloudCSVMongoAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_CLOUD_CSV_MONGO)
		return nil, st.Err()
	}

	jobCfg := batch.MapCloudCSVMongoBatchConfigFromProto(req.GetCloudCsvMongoConfig())

	batReq := batch.CloudCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				BatchSize:           uint(req.BatchSize),
				MaxBatches:          uint(req.MaxBatches),
				Start:               req.Start,
				MaxInProcessBatches: uint(req.MaxInProcessBatches),
				MappingRules:        batch.MapRulesFromProto(req.MappingRules),
			},
		},
		Config: jobCfg,
	}

	resp, err := s.SchedulerService.ProcessCloudCSVToMongoWorkflow(ctx, batReq)
	if err != nil {
		l.Error("error processing cloud csv to mongo workflow", "error", err.Error(), "req", req)
		st := status.New(codes.Internal, err.Error())
		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	l.Debug("created local csv to mongo workflow run", "resp", resp)
	return &api.RunResponse{
		Ok:  true,
		Run: stores.MapWorkflowRunToProto(resp),
	}, nil
}

// Workflow Runs

func (s *grpcServer) CreateRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		createRunAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_CREATE_RUN)
		return nil, st.Err()
	}

	resp, err := s.SchedulerService.CreateRun(ctx, &stores.WorkflowRun{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Type:        req.Type,
		ExternalRef: req.ExternalRef,
	})
	if err != nil {
		l.Error("error creating workflow run", "error", err.Error(), "req", req)
		st := status.New(codes.Internal, err.Error())
		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	l.Debug("create workflow run response", "resp", resp)
	return &api.RunResponse{
		Ok:  true,
		Run: stores.MapWorkflowRunToProto(resp),
	}, nil
}

func (s *grpcServer) GetRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		getRunAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_GET_RUN)
		return nil, st.Err()
	}

	resp, err := s.SchedulerService.GetRun(ctx, req.RunId)
	if err != nil {
		l.Error("error fetching run details", "error", err.Error())
		st := status.New(codes.Internal, err.Error())

		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}

	l.Debug("workflow run details", "resp", resp)
	return &api.RunResponse{
		Ok:  true,
		Run: stores.MapWorkflowRunToProto(resp),
	}, nil
}

func (s *grpcServer) DeleteRun(ctx context.Context, req *api.DeleteRunRequest) (*api.DeleteResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		deleteRunAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_DELETE_RUN)
		return nil, st.Err()
	}

	if err := s.SchedulerService.DeleteRun(ctx, req.Id); err != nil {
		l.Error("error deleting workflow run", "error", err.Error(), "runId", req.Id)
		st := status.New(codes.Internal, err.Error())
		return nil, st.Err()
	}

	return &api.DeleteResponse{
		Ok: true,
	}, nil
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		fmt.Println("authenticate: Peer info nil")
		return ctx, status.New(
			codes.PermissionDenied,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		fmt.Println("authenticate: Auth info nil")
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	fmt.Println("authenticate: Auth subject: ", subject)
	return ctx, nil
}

func decorateContext(ctx context.Context) (context.Context, error) {
	_, err := logger.LoggerFromContext(ctx)
	if err != nil {
		// If there's no logger in the context, add a new one
		// TODO: setup context
		fmt.Println("decorateContext: missing logger in context, providing default logger")
		ctx = logger.WithLogger(ctx, logger.GetSlogLogger())
	}
	return ctx, nil
}

func metadataLogger(ctx context.Context) (context.Context, error) {
	if md, ok := metadata.FromIncomingContext(ctx); !ok {
		fmt.Println("metadataLogger: missing metadata")
	} else {
		fmt.Println("metadataLogger: metadata found: ", md)
	}
	return ctx, nil
}
