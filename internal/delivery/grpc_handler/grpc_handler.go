package grpchandler

import (
	"context"
	"fmt"
	"io"
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

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/scheduler"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

var _ api.SchedulerServer = (*grpcServer)(nil)

const (
	ERR_UNAUTHORIZED_CREATE_RUN      = "unauthorized to create run"
	ERR_UNAUTHORIZED_GET_RUN         = "unauthorized to get run"
	ERR_UNAUTHORIZED_DELETE_RUN      = "unauthorized to delete run"
	ERR_UNAUTHORIZED_LOCAL_CSV_MONGO = "unauthorized to process local csv to mongo workflow"
	ERR_UNAUTHORIZED_CLOUD_CSV_MONGO = "unauthorized to process cloud csv to mongo workflow"
	ERR_UNAUTHORIZED_ADD_ENTITY      = "unauthorized to add entity"
	ERR_UNAUTHORIZED_GET_ENTITY      = "unauthorized to get entity"
	ERR_UNAUTHORIZED_DELETE_ENTITY   = "unauthorized to delete entity"
)

const (
	objectWildcard      = "*"
	createRunAction     = "create-run"
	getRunAction        = "get-run"
	deleteRunAction     = "delete-run"
	localCSVMongoAction = "local-csv-mongo"
	cloudCSVMongoAction = "cloud-csv-mongo"
	addEntityAction     = "add-entity"
	getEntityAction     = "get-entity"
	deleteEntityAction  = "delete-entity"
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
		return nil, fmt.Errorf("SchedulerServer:ProcessLocalCSVMongoWorkflow - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		localCSVMongoAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_LOCAL_CSV_MONGO)
		return nil, st.Err()
	}

	reqCfg, err := envutils.BuildLocalCSVMongoBatchConfig()
	if err != nil {
		return nil, err
	}

	batReq := batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: uint(req.MaxBatches),
				BatchSize:  uint(req.BatchSize),
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: reqCfg,
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
		return nil, fmt.Errorf("SchedulerServer:ProcessCloudCSVMongoWorkflow - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		cloudCSVMongoAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_CLOUD_CSV_MONGO)
		return nil, st.Err()
	}

	reqCfg, err := envutils.BuildCloudCSVMongoBatchConfig()
	if err != nil {
		return nil, err
	}

	batReq := batch.CloudCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: uint(req.MaxBatches),
				BatchSize:  uint(req.BatchSize),
				Offsets:    []uint64{},
				Headers:    []string{},
				Mappings:   req.Mappings,
			},
		},
		Config: reqCfg,
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
		return nil, fmt.Errorf("SchedulerServer:CreateRun - %w", err)
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
		return nil, fmt.Errorf("SchedulerServer:GetRun - %w", err)
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
		return nil, fmt.Errorf("SchedulerServer:DeleteRun - %w", err)
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

// Business Entities

func (s *grpcServer) AddBusinessEntities(stream api.Scheduler_AddBusinessEntitiesServer) error {
	ctx := stream.Context()

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("SchedulerServer:AddBusinessEntities - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		addEntityAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_ADD_ENTITY)
		return st.Err()
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				l.Info("stream ended, returning")
				return nil
			}
			return err
		}

		var stResp api.StreamEntityResponse
		if res, err := s.AddEntity(stream.Context(), req); err != nil {
			l.Error("error adding entity", "error", err.Error())
			if st, ok := status.FromError(err); ok {
				l.Error("error adding entity", "error-status", st)
			}

			stResp = api.StreamEntityResponse{
				TestOneof: &api.StreamEntityResponse_Error{
					Error: err.Error(),
				},
			}
		} else {
			stResp = api.StreamEntityResponse{
				TestOneof: &api.StreamEntityResponse_EntityResponse{
					EntityResponse: res,
				},
			}
		}

		if err = stream.Send(&stResp); err != nil {
			l.Error("error sending response", "error", err.Error())
			return err
		}
	}
}

func (s *grpcServer) AddEntity(ctx context.Context, req *api.AddEntityRequest) (*api.EntityResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerServer:GetEntity - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		addEntityAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_ADD_ENTITY)
		return nil, st.Err()
	}

	resp := api.EntityResponse{}
	switch req.Type {
	case api.EntityType_AGENT:
		if entityId, entity, err := s.SchedulerService.AddAgent(ctx, stores.MapAgentFieldsToMongoModel(req.Fields)); err != nil {
			l.Error("error adding agent entity", "error", err.Error())
			st := status.New(codes.FailedPrecondition, err.Error())
			return nil, st.Err()
		} else {
			resp.TestOneof = &api.EntityResponse_Agent{
				Agent: stores.MapAgentModelToProto(entity, entityId),
			}
		}
	case api.EntityType_FILING:
		if entityId, entity, err := s.SchedulerService.AddFiling(ctx, stores.MapFilingFieldsToMongoModel(req.Fields)); err != nil {
			l.Error("error adding filing entity", "error", err.Error())
			st := status.New(codes.FailedPrecondition, err.Error())
			return nil, st.Err()
		} else {
			resp.TestOneof = &api.EntityResponse_Filing{
				Filing: stores.MapFilingModelToProto(entity, entityId),
			}
		}
	default:
		l.Error("unsupported entity type for fetching", "entityType", req.Type)
		st := status.New(codes.InvalidArgument, "unsupported entity type for fetching")
		return nil, st.Err()
	}

	resp.Ok = true
	return &resp, nil
}

func (s *grpcServer) GetEntity(ctx context.Context, req *api.EntityRequest) (*api.EntityResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerServer:GetEntity - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		getEntityAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_GET_ENTITY)
		return nil, st.Err()
	}

	resp := api.EntityResponse{}
	switch req.Type {
	case api.EntityType_AGENT:
		if entity, err := s.SchedulerService.GetAgent(ctx, req.Id); err != nil {
			l.Error("error fetching agent entity", "error", err.Error(), "id", req.Id)
			st := status.New(codes.Internal, "error fetching agent entity")
			return nil, st.Err()
		} else {
			resp.TestOneof = &api.EntityResponse_Agent{
				Agent: stores.MapAgentModelToProto(entity, req.Id),
			}
		}
	case api.EntityType_FILING:
		if entity, err := s.SchedulerService.GetFiling(ctx, req.Id); err != nil {
			l.Error("error fetching filing entity", "error", err.Error(), "id", req.Id)
			st := status.New(codes.Internal, "error fetching filing entity")
			return nil, st.Err()
		} else {
			resp.TestOneof = &api.EntityResponse_Filing{
				Filing: stores.MapFilingModelToProto(entity, req.Id),
			}
		}
	default:
		l.Error("unsupported entity type for fetching", "entityType", req.Type)
		st := status.New(codes.InvalidArgument, "unsupported entity type for fetching")
		return nil, st.Err()
	}

	resp.Ok = true
	return &resp, nil
}

func (s *grpcServer) DeleteEntity(ctx context.Context, req *api.EntityRequest) (*api.DeleteResponse, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerServer:DeleteEntity - %w", err)
	}

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		deleteEntityAction,
	); err != nil {
		st := status.New(codes.Unauthenticated, ERR_UNAUTHORIZED_DELETE_ENTITY)
		return nil, st.Err()
	}

	ok, err := s.SchedulerService.DeleteEntity(ctx, stores.MapEntityTypeToModel(req.Type), req.Id)
	if err != nil {
		l.Error("error deleting business entity", "error", err.Error(), "id", req.Id)
		st := status.New(codes.Internal, "error deleting business entity")

		return nil, st.Err()
	}

	return &api.DeleteResponse{
		Ok: ok,
	}, nil
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

func decorateContext(ctx context.Context) (context.Context, error) {
	_, err := logger.LoggerFromContext(ctx)
	if err != nil {
		// If there's no logger in the context, add a new one
		// TODO: setup context
		ctx = logger.WithLogger(ctx, logger.GetSlogLogger())
	}
	return ctx, nil
}

func metadataLogger(ctx context.Context) (context.Context, error) {
	_, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		fmt.Println("missing metadata")
	}
	return ctx, nil
}
