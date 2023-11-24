package file

import (
	"time"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

// ApplicationName is the task list for this workflow
const ReadCSVWorkflowName = "github.com/hankgalt/workflow-scheduler/pkg/workflows/file.ReadCSVWorkflow"

// ReadCSVWorkflow workflow decider
func ReadCSVWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ReadCSVWorkflow started.")

	count := 0
	resp, err := readCSV(ctx, req)
	for err != nil && count < 10 {
		count++
		switch err {
		case common.ErrSessionCtx:
			resp, err = readCSV(ctx, req)
			continue
		default:
			logger.Error(common.ERR_UNHANDLED, zap.Error(err), zap.String("err-msg", err.Error()))
			resp, err = readCSV(ctx, req)
			continue
		}
	}

	if err != nil {
		logger.Error(
			"ReadCSVWorkflow failed.",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
		)
		return req, errors.WrapError(err, "ReadCSVWorkflow failed.")
	}

	logger.Info("ReadCSVWorkflow completed.")
	return resp, nil
}

func readCSV(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := workflow.GetLogger(ctx)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: 3 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		logger.Error(common.ERR_SESSION_CTX, zap.String("err-msg", err.Error()))
		return nil, common.ErrSessionCtx
	}
	defer workflow.CompleteSession(sessionCtx)

	runId, wkflId := common.GetRunWorkflowIds(ctx, req.RunId, req.WorkflowId)

	resp := models.RequestInfo{
		RunId:       runId,
		WorkflowId:  wkflId,
		RequestedBy: req.RequestedBy,
		FileName:    req.FileName,
		Org:         req.Org,
		Status:      req.Status,
		Count:       req.Count,
		Processed:   req.Processed,
	}

	runInfo, err := ExecuteReadCSVActivity(sessionCtx, req)
	if err != nil {
		logger.Error("readCSV failed - error creating file", zap.Error(err))
		return nil, err
	}
	logger.Info("readCSV - workflow done", zap.String("filepath", req.FileName))
	resp.Count = runInfo.Count
	return &resp, nil
}
