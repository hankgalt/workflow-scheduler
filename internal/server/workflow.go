package server

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	structpb "google.golang.org/protobuf/types/known/structpb"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

func (s *grpcServer) ProcessFileSignalWorkflow(ctx context.Context, req *api.FileSignalRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		fileSignalAction,
	); err != nil {
		return nil, err
	}

	resp, err := s.SchedulerService.ProcessFileSignalWorkflow(ctx, &models.FileSignalParams{
		FilePath:    req.FilePath,
		Type:        models.MapProtoToEntityType(req.Type),
		RequestedBy: req.RequestedBy,
	})
	if err != nil {
		s.Error("error processing file signal workflow", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "workflow",
			err:      err,
			errTxt:   "error processing file signal workflow",
			errCode:  codes.FailedPrecondition,
		})

		return &api.RunResponse{
			Ok: false,
		}, st.Err()
	}
	s.Info("created workflow run response", zap.Any("resp", resp))
	return &api.RunResponse{
		Ok:  true,
		Run: models.MapToRunProto(resp),
	}, nil
}

func (s *grpcServer) QueryWorkflowState(ctx context.Context, req *api.QueryWorkflowRequest) (*api.QueryWorkflowResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		queryWorkflowAction,
	); err != nil {
		return nil, err
	}

	resp, err := s.SchedulerService.QueryWorkflowState(ctx, &models.WorkflowQueryParams{
		RunId:      req.RunId,
		WorkflowId: req.WorkflowId,
	})
	if err != nil {
		s.Error("error querying workflow state", zap.Error(err), zap.Any("req", req))
		st := s.buildError(errorParams{
			errModel: "workflow",
			err:      err,
			errTxt:   "error querying workflow state",
			errCode:  codes.FailedPrecondition,
		})

		return &api.QueryWorkflowResponse{
			Ok: false,
		}, st.Err()
	}

	state, ok := resp.(map[string]interface{})
	if !ok {
		st := s.buildError(errorParams{
			errModel: "workflow",
			err:      err,
			errTxt:   "error casting workflow state",
			errCode:  codes.FailedPrecondition,
		})

		return &api.QueryWorkflowResponse{
			Ok: false,
		}, st.Err()
	}

	s.Info("queried workflow state", zap.Any("resp", state))

	if state["FileSize"] != nil {
		fileSize := state["FileSize"]
		s.Info("queried workflow state", zap.Any("file-size", fileSize))
		state["FileSize"] = fmt.Sprintf("%v", fileSize)
	}

	if fields, err := structpb.NewStruct(state); err != nil {
		st := s.buildError(errorParams{
			errModel: "workflow",
			err:      err,
			errTxt:   "error converting workflow state to struct",
			errCode:  codes.FailedPrecondition,
		})
		return &api.QueryWorkflowResponse{
			Ok: false,
		}, st.Err()
	} else {
		return &api.QueryWorkflowResponse{
			Ok:    true,
			State: fields,
		}, nil
	}
}
