package server

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"google.golang.org/grpc/status"
)

func (s *grpcServer) AddBusinessEntities(stream api.Scheduler_AddBusinessEntitiesServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.Logger.Info("stream ended, returning")
				return nil
			}
			return err
		}

		var stResp api.StreamAddEntityResponse
		if res, err := s.AddEntity(stream.Context(), req); err != nil {
			s.Logger.Error("error adding entity", zap.Error(err))
			_, ok := status.FromError(err)
			if ok {
				stResp = api.StreamAddEntityResponse{
					TestOneof: &api.StreamAddEntityResponse_Error{
						Error: err.Error(),
					},
				}
			} else {
				stResp = api.StreamAddEntityResponse{
					TestOneof: &api.StreamAddEntityResponse_Error{
						Error: err.Error(),
					},
				}
			}
		} else {
			stResp = api.StreamAddEntityResponse{
				TestOneof: &api.StreamAddEntityResponse_EntityResponse{
					EntityResponse: res,
				},
			}
		}

		if err = stream.Send(&stResp); err != nil {
			s.Error("error sending response", zap.Error(err))
			return err
		}
	}
}

func (s *grpcServer) AddEntity(ctx context.Context, req *api.AddEntityRequest) (*api.AddEntityResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		addEntityAction,
	); err != nil {
		return nil, err
	}
	s.Debug("AddEntity", zap.Any("fields", req.Fields), zap.Any("type", req.Type))
	resp := api.AddEntityResponse{}
	var err error
	var errModel string
	switch req.Type {
	case api.EntityType_AGENT:
		errModel = "agent"
		if ag, agErr := s.SchedulerService.AddAgent(ctx, models.MapAgentFieldsToModel(req.Fields)); agErr == nil {
			resp.TestOneof = &api.AddEntityResponse_Agent{
				Agent: models.MapAgentModelToProto(ag),
			}
		} else {
			err = agErr
		}
	case api.EntityType_PRINCIPAL:
		errModel = "principal"
		if bp, prErr := s.SchedulerService.AddPrincipal(ctx, models.MapPrincipalFieldsToModel(req.Fields)); prErr == nil {
			resp.TestOneof = &api.AddEntityResponse_Principal{
				Principal: models.MapPrincipalModelToProto(bp),
			}
		} else {
			err = prErr
		}
	case api.EntityType_FILING:
		errModel = "filing"
		if bf, fiErr := s.SchedulerService.AddFiling(ctx, models.MapFilingFieldsToModel(req.Fields)); fiErr == nil {
			resp.TestOneof = &api.AddEntityResponse_Filing{
				Filing: models.MapFilingModelToProto(bf),
			}
		} else {
			err = fiErr
		}
	}

	if err != nil {
		s.Error("error adding business entity", zap.Error(err), zap.Any("type", req.Type))
		st := s.buildError(errorParams{
			errModel: errModel,
			err:      err,
			errTxt:   fmt.Sprintf("error adding %s business entity", errModel),
			errCode:  codes.FailedPrecondition,
		})

		resp.Ok = false
		return &resp, st.Err()
	}
	resp.Ok = true
	return &resp, nil
}

func (s *grpcServer) DeleteEntity(ctx context.Context, req *api.EntityRequest) (*api.DeleteResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		deleteEntityAction,
	); err != nil {
		return nil, err
	}

	var err error
	var errModel string
	switch req.Type {
	case api.EntityType_AGENT:
		errModel = "agent"
		err = s.SchedulerService.DeleteAgent(ctx, req.Id)
	case api.EntityType_PRINCIPAL:
		errModel = "principal"
		err = s.SchedulerService.DeletePrincipal(ctx, req.Id)
	case api.EntityType_FILING:
		errModel = "filing"
		err = s.SchedulerService.DeleteFiling(ctx, req.Id)
	}

	if err != nil {
		s.Logger.Info("error deleting business entity", zap.Error(err), zap.Any("type", req.Type))
		st := s.buildError(errorParams{
			errModel: errModel,
			err:      err,
			errTxt:   fmt.Sprintf("error deleting %s business entity", errModel),
			errCode:  codes.Internal,
		})

		return nil, st.Err()
	}

	return &api.DeleteResponse{
		Ok: true,
	}, nil
}

func (s *grpcServer) GetEntity(ctx context.Context, req *api.EntityRequest) (*api.EntityResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		getEntityAction,
	); err != nil {
		return nil, err
	}

	var err error
	var errModel string
	resp := api.EntityResponse{}
	switch req.Type {
	case api.EntityType_AGENT:
		errModel = "agent"
		if ag, agErr := s.SchedulerService.GetAgent(ctx, req.Id); agErr == nil {
			resp.TestOneof = &api.EntityResponse_Agent{
				Agent: models.MapAgentModelToProto(ag),
			}
		} else {
			err = agErr
		}
		break
	case api.EntityType_PRINCIPAL:
		errModel = "principal"
		if ag, agErr := s.SchedulerService.GetPrincipal(ctx, req.Id); agErr == nil {
			resp.TestOneof = &api.EntityResponse_Principal{
				Principal: models.MapPrincipalModelToProto(ag),
			}
		} else {
			err = agErr
		}
		break
	case api.EntityType_FILING:
		errModel = "filing"
		if ag, agErr := s.SchedulerService.GetFiling(ctx, req.Id); agErr == nil {
			resp.TestOneof = &api.EntityResponse_Filing{
				Filing: models.MapFilingModelToProto(ag),
			}
		} else {
			err = agErr
		}
		break
	}

	if err != nil {
		s.Logger.Info("error fetching business entity", zap.Error(err), zap.Any("type", req.Type))
		st := s.buildError(errorParams{
			errModel: errModel,
			err:      err,
			errTxt:   fmt.Sprintf("error fetching %s business entity", errModel),
			errCode:  codes.FailedPrecondition,
		})

		resp.Ok = false
		return &resp, st.Err()
	}
	resp.Ok = true
	return &resp, nil
}
