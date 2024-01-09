package business

import (
	"fmt"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

// ProcessFileSignalWorkflow workflow processor
func ProcessFileSignalWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ProcessFileSignalWorkflow started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))

	count := 0
	configErr := false
	resp, err := processFileSignal(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *workflow.GenericError:
			l.Error("cadence generic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *workflow.TimeoutError:
			l.Error("time out error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return req, err
		case *cadence.CustomError:
			l.Error("cadence custom error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Reason() {
			case common.ERR_SESSION_CTX:
				resp, err = processFileSignal(ctx, resp)
				continue
			case common.ERR_WRONG_HOST:
				configErr = true
				return req, err
			case common.ERR_MISSING_FILE_NAME:
				configErr = true
				return req, err
			case common.ERR_MISSING_REQSTR:
				configErr = true
				return req, err
			case ERR_MISSING_OFFSETS:
				configErr = true
				return req, err
			default:
				resp, err = processFileSignal(ctx, resp)
				continue
			}
		case *workflow.PanicError:
			l.Error("cadence panic error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		case *cadence.CanceledError:
			l.Error("cadence canceled error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			configErr = true
			return resp, err
		default:
			l.Error("other error", zap.Error(err), zap.String("err-msg", err.Error()), zap.String("type", fmt.Sprintf("%T", err)))
			resp, err = processFileSignal(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessFileSignalWorkflow - failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("ProcessFileSignalWorkflow failed", err)
	}

	l.Info(
		"ProcessFileSignalWorkflow - completed",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

func processFileSignal(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ProcessFileSignalWorkflow - starting execution", zap.String("file", req.FileName))

	// set execution duration
	executionDuration := common.ONE_DAY

	// build session context
	so := &workflow.SessionOptions{
		CreationTimeout:  10 * time.Minute,
		ExecutionTimeout: executionDuration,
	}
	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		l.Error(common.ERR_SESSION_CTX, zap.Error(err))
		return req, cadence.NewCustomError(common.ERR_SESSION_CTX, err)
	}
	sessionCtx = workflow.WithStartToCloseTimeout(sessionCtx, executionDuration)
	defer workflow.CompleteSession(sessionCtx)

	runId := workflow.GetInfo(ctx).WorkflowExecution.RunID
	wkflId := workflow.GetInfo(ctx).WorkflowExecution.ID
	l.Debug("ProcessFileSignalWorkflow - current workflow execution", zap.String("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))

	if runs, err := common.ExecuteSearchRunActivity(sessionCtx, &models.RunParams{
		Type:        ProcessFileSignalWorkflowName,
		ExternalRef: req.FileName,
	}); err != nil {
		l.Error("ProcessFileSignalWorkflow - no corresponding run state found, creating new run state", zap.Error(err), zap.String("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))

		// create a new run
		if runInfo, err := common.ExecuteCreateRunActivity(sessionCtx, &models.RunParams{
			RunId:       runId,
			WorkflowId:  wkflId,
			RequestedBy: req.RequestedBy,
			ExternalRef: req.FileName,
			Type:        ProcessFileSignalWorkflowName,
		}); err != nil {
			l.Error(common.ERR_CREATING_WKFL_RUN, zap.Error(err), zap.String("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))
			return req, err
		} else {
			l.Info(common.WKFL_RUN_CREATED, zap.String("run-id", runInfo.RunId), zap.String("wkfl-id", runInfo.WorkflowId), zap.String("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))
			req.RunId = runInfo.RunId
			req.WorkflowId = runInfo.WorkflowId
		}
	} else {
		l.Debug("ProcessFileSignalWorkflow - found existing run state", zap.Any("run", runs[0]), zap.String("file", req.FileName), zap.String("run-id", runs[0].RunId), zap.String("wkfl-id", runs[0].WorkflowId))
		req.RunId = runs[0].RunId
		req.WorkflowId = runs[0].WorkflowId
	}

	l.Debug("ProcessFileSignalWorkflow - starting file download", zap.String("file", req.FileName), zap.String("run-id", req.RunId), zap.String("wkfl-id", req.WorkflowId))
	// download file
	if _, err := fiwkfl.ExecuteDownloadFileActivity(sessionCtx, &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
	}); err != nil {
		l.Error("ProcessFileSignalWorkflow - error dowloading file", zap.String("error", err.Error()), zap.Any("file", req.FileName))
		return req, err
	}
	l.Debug("ProcessFileSignalWorkflow - file downloaded, starting process CSV workflow", zap.Any("file", req.FileName))

	// build child workflow context
	scwo := workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: executionDuration,
	}
	cwCtx := workflow.WithChildOptions(sessionCtx, scwo)

	// start CSV processing workflow to process file
	req.Type = api.EntityType_AGENT
	future := workflow.ExecuteChildWorkflow(cwCtx, ProcessCSVWorkflow, req)
	var resp models.CSVInfo
	err = future.Get(sessionCtx, &resp)
	if err != nil {
		l.Error("ProcessFileSignalWorkflow - error executing ProcessCSVWorkflow child workflow", zap.Error(err), zap.String("file", req.FileName))
	} else {
		errCount := 0
		resultCount := 0
		recCount := 0
		for _, v := range req.Results {
			errCount = errCount + v.ErrCount
			resultCount = resultCount + v.ResultCount
			recCount = recCount + v.Count
		}
		l.Debug(
			"ProcessFileSignalWorkflow - ProcessCSVWorkflow response",
			zap.Int64("size", req.FileSize),
			zap.Int("batches", len(resp.OffSets)),
			zap.Int("batches-processed", len(resp.Results)),
			zap.Int("errCount", errCount),
			zap.Int("resultCount", resultCount),
			zap.Int("recCount", recCount))
	}
	l.Debug("ProcessFileSignalWorkflow - file processed, updating run state", zap.Any("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))

	// update run
	if runInfo, err := common.ExecuteUpdateRunActivity(sessionCtx, &models.RunParams{
		RunId:       runId,
		WorkflowId:  wkflId,
		RequestedBy: req.RequestedBy,
		Status:      string(models.COMPLETED),
	}); err != nil {
		l.Error(common.ERR_UPDATING_WKFL_RUN, zap.Error(err), zap.Any("file", req.FileName), zap.String("run-id", runId), zap.String("wkfl-id", wkflId))
		return &resp, err
	} else {
		l.Debug(common.WKFL_RUN_UPDATED, zap.String("run-id", runInfo.RunId), zap.String("wkfl-id", runInfo.WorkflowId))
	}

	return &resp, nil
}
