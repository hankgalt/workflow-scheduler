package server

import (
	"context"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	// "github.com/hankgalt/workflow-scheduler/pkg/models"
)

func (s *grpcServer) CreateRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		createRunAction,
	); err != nil {
		return nil, err
	}

	// resp, err := s.SchedulerService.CreateRun(ctx, &models.RunParams{
	// 	RunId:       req.RunId,
	// 	WorkflowId:  req.WorkflowId,
	// 	Type:        req.Type,
	// 	ExternalRef: req.ExternalRef,
	// 	RequestedBy: req.RequestedBy,
	// })
	// if err != nil {
	// 	s.Error("error creating workflow run record", zap.Error(err), zap.Any("req", req))
	// 	st := s.buildError(errorParams{
	// 		errModel: "run",
	// 		err:      err,
	// 		errTxt:   "error creating workflow run",
	// 		errCode:  codes.FailedPrecondition,
	// 	})

	// 	return &api.RunResponse{
	// 		Ok: false,
	// 	}, st.Err()
	// }
	// s.Info("create workflow run response", zap.Any("resp", resp))
	// return &api.RunResponse{
	// 	Ok:  true,
	// 	Run: models.MapToRunProto(resp),
	// }, nil
	return nil, nil
}

func (s *grpcServer) UpdateRun(ctx context.Context, req *api.UpdateRunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		updateRunAction,
	); err != nil {
		return nil, err
	}

	// resp, err := s.SchedulerService.UpdateRun(ctx, &models.RunParams{
	// 	RunId:      req.RunId,
	// 	WorkflowId: req.WorkflowId,
	// 	Status:     req.Status,
	// })
	// if err != nil {
	// 	s.Error("error updating run status", zap.Error(err), zap.Any("req", req))
	// 	st := s.buildError(errorParams{
	// 		errModel: "run",
	// 		err:      err,
	// 		errTxt:   "error updating workflow run",
	// 		errCode:  codes.FailedPrecondition,
	// 	})

	// 	return &api.RunResponse{
	// 		Ok: false,
	// 	}, st.Err()
	// }
	// s.Info("workflow run update response", zap.Any("resp", resp))
	// return &api.RunResponse{
	// 	Ok:  true,
	// 	Run: models.MapToRunProto(resp),
	// }, nil

	return nil, nil
}

func (s *grpcServer) GetRun(ctx context.Context, req *api.RunRequest) (*api.RunResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		getRunAction,
	); err != nil {
		return nil, err
	}

	// 	resp, err := s.SchedulerService.GetRun(ctx, &models.RunParams{
	// 		RunId:       req.RunId,
	// 		WorkflowId:  req.WorkflowId,
	// 		RequestedBy: req.RequestedBy,
	// 	})
	// 	if err != nil {
	// 		s.Error("error fetching run details", zap.Error(err), zap.Any("req", req))
	// 		st := s.buildError(errorParams{
	// 			errModel: "run",
	// 			err:      err,
	// 			errTxt:   "error fetching workflow run",
	// 			errCode:  codes.FailedPrecondition,
	// 		})

	// 		return &api.RunResponse{
	// 			Ok: false,
	// 		}, st.Err()
	// 	}
	// 	s.Config.Info("workflow run details", zap.Any("resp", resp))
	// 	return &api.RunResponse{
	// 		Ok:  true,
	// 		Run: models.MapToRunProto(resp),
	// 	}, nil
	return nil, nil
}

func (s *grpcServer) DeleteRun(ctx context.Context, req *api.DeleteRunRequest) (*api.DeleteResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		deleteRunAction,
	); err != nil {
		return nil, err
	}

	// 	err := s.SchedulerService.DeleteRun(ctx, req.Id)
	// 	if err != nil {
	// 		s.Error("error deleting run", zap.Error(err), zap.Any("req", req))
	// 		st := s.buildError(errorParams{
	// 			errModel: "run",
	// 			err:      err,
	// 			errTxt:   "error deleting workflow run",
	// 			errCode:  codes.FailedPrecondition,
	// 		})

	// 		return nil, st.Err()
	// 	}

	// 	s.Debug("run deleted", zap.Any("id", req.Id))
	// 	return &api.DeleteResponse{
	// 		Ok: true,
	// 	}, nil
	return nil, nil
}

func (s *grpcServer) SearchRuns(ctx context.Context, req *api.SearchRunRequest) (*api.RunsResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		searchRunsAction,
	); err != nil {
		return nil, err
	}

	// 	resp, err := s.SchedulerService.SearchRuns(ctx, &models.RunParams{
	// 		RunId:       req.RunId,
	// 		WorkflowId:  req.WorkflowId,
	// 		Type:        req.Type,
	// 		Status:      req.Status,
	// 		ExternalRef: req.ExternalRef,
	// 	})
	// 	if err != nil {
	// 		s.Error("error searching workflow run records", zap.Error(err), zap.Any("req", req))
	// 		st := s.buildError(errorParams{
	// 			errModel: "run",
	// 			err:      err,
	// 			errTxt:   "error searching workflow run",
	// 			errCode:  codes.FailedPrecondition,
	// 		})

	// 		return &api.RunsResponse{
	// 			Ok: false,
	// 		}, st.Err()
	// 	}
	// 	s.Info("search workflow run response", zap.Any("resp", resp), zap.Any("len", len(resp)))
	// 	if len(resp) == 0 {
	// 		s.Error("no workflow run records", zap.Error(err), zap.Any("req", req))
	// 		st := s.buildError(errorParams{
	// 			errModel: "run",
	// 			err:      err,
	// 			errTxt:   "no workflow run",
	// 			errCode:  codes.FailedPrecondition,
	// 		})
	// 		return &api.RunsResponse{
	// 			Ok: false,
	// 		}, st.Err()
	// 	}
	// 	return &api.RunsResponse{
	// 		Ok:   true,
	// 		Runs: models.MapToRunProtos(resp),
	// 	}, nil
	return nil, nil
}
