package common

import (
	"log/slog"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/cadence"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

// CreateRunWorkflow workflow decider
func CreateRunWorkflow(ctx workflow.Context, req *models.RunParams) (*models.RunParams, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"CreateRunWorkflow - started",
		slog.String("run-id", req.RunId),
		slog.String("wkfl-id", req.WorkflowId),
		slog.String("type", req.Type),
		slog.String("reqstr", req.RequestedBy))

	resp, err := createRun(ctx, req)

	if err != nil {
		l.Error(
			"CreateRunWorkflow - failed",
			slog.String("err-msg", err.Error()),
		)
		return resp, cadence.NewCustomError("ReadCSVWorkflow failed", err)
	}

	l.Info(
		"CreateRunWorkflow - completed",
		slog.String("run-id", req.RunId),
		slog.String("wkfl-id", req.WorkflowId),
		slog.String("type", req.Type),
		slog.String("reqstr", req.RequestedBy))
	return resp, nil
}

func createRun(ctx workflow.Context, req *models.RunParams) (*models.RunParams, error) {
	l := workflow.GetLogger(ctx)

	resp, err := ExecuteCreateRunActivity(ctx, req)
	if err != nil {
		l.Error("createRun - error executing create run activity", slog.Any("error", err), slog.String("run-id", req.RunId), slog.String("wkfl-id", req.WorkflowId))
		return req, err
	}
	l.Debug("createRun - create activity response", slog.String("run-id", req.RunId), slog.String("wkfl-id", req.WorkflowId), slog.Any("resp", resp))
	return req, nil
}
