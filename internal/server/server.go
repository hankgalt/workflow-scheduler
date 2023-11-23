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

	"github.com/comfforts/errors"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

var _ api.SchedulerServer = (*grpcServer)(nil)

const (
	objectWildcard  = "*"
	createRunAction = "create-run"
	updateRunAction = "update-run"
	getRunAction    = "get-run"
	deleteRunAction = "delete-run"
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
	Authorizer       Authorizer
	SchedulerService scheduler.SchedulerService
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
		return nil, errors.WrapError(err, "error getting host name")
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

func (s *grpcServer) CreateRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		createRunAction,
	); err != nil {
		return nil, err
	}

	resp, err := s.SchedulerService.CreateRun(ctx, &models.RunUpdateParams{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		RequestedBy: req.RequestedBy,
	})
	if err != nil {
		s.Error("error creating workflow run record", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "run",
			err:      err,
			errTxt:   "error creating workflow run",
			errCode:  codes.FailedPrecondition,
		})

		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	s.Info("create workflow run response", zap.Any("resp", resp))
	return &api.RunResponse{
		Ok: true,
		Run: &api.WorkflowRun{
			RunId:      resp.RunId,
			WorkflowId: resp.WorkflowId,
			Status:     resp.Status,
		},
	}, nil
}

func (s *grpcServer) UpdateRun(ctx context.Context, req *api.UpdateRunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		updateRunAction,
	); err != nil {
		return nil, err
	}

	resp, err := s.SchedulerService.UpdateRun(ctx, &models.RunUpdateParams{
		RunId:      req.RunId,
		WorkflowId: req.WorkflowId,
		Status:     req.Status,
	})
	if err != nil {
		s.Error("error updating run status", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "run",
			err:      err,
			errTxt:   "error updating workflow run",
			errCode:  codes.FailedPrecondition,
		})

		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	s.Info("workflow run update response", zap.Any("resp", resp))
	return &api.RunResponse{
		Ok: true,
		Run: &api.WorkflowRun{
			RunId:      resp.RunId,
			WorkflowId: resp.WorkflowId,
			Status:     resp.Status,
		},
	}, nil
}

func (s *grpcServer) GetRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		getRunAction,
	); err != nil {
		return nil, err
	}

	resp, err := s.SchedulerService.GetRun(ctx, &models.RunUpdateParams{
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		RequestedBy: req.RequestedBy,
	})
	if err != nil {
		s.Error("error fetching run details", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "run",
			err:      err,
			errTxt:   "error fetching workflow run",
			errCode:  codes.FailedPrecondition,
		})

		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	s.Config.Info("workflow run details", zap.Any("resp", resp))
	return &api.RunResponse{
		Ok: true,
		Run: &api.WorkflowRun{
			RunId:      resp.RunId,
			WorkflowId: resp.WorkflowId,
			Status:     resp.Status,
		},
	}, nil
}

func (s *grpcServer) DeleteRun(ctx context.Context, req *api.DeleteRunRequest) (*api.DeleteResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		deleteRunAction,
	); err != nil {
		return nil, err
	}

	err := s.SchedulerService.DeleteRun(ctx, req.Id)
	if err != nil {
		s.Error("error deleting run", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "run",
			err:      err,
			errTxt:   "error deleting workflow run",
			errCode:  codes.FailedPrecondition,
		})

		return nil, st.Err()
	}

	s.Debug("run deleted", zap.Any("id", req.Id))
	return &api.DeleteResponse{
		Ok: true,
	}, nil
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

func (s *grpcServer) buildError(params errorParams) *status.Status {
	appErr, ok := params.err.(errors.AppError)
	if !ok {
		msg := fmt.Sprintf("%s - %s", params.errTxt, "unknown error")
		s.Error(msg, zap.Error(params.err))
		return status.New(codes.Unknown, msg)
	}

	switch params.errModel {
	case "run":
		if appErr == scheduler.ErrDuplicateRun {
			return status.New(codes.AlreadyExists, appErr.Message)
		}
		return status.New(params.errCode, appErr.Message)
	default:
		return status.New(params.errCode, appErr.Message)
	}
}
