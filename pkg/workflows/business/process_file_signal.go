package business

import (
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
)

const (
	ERR_PROCESS_FILE_WKFL string = "error process file signal workflow"
)

// ProcessFileSignalWorkflow workflow processor
func ProcessFileSignalWorkflow(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"ProcessFileSignalWorkflow started",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy))

	count := 0
	resp, err := processFileSignal(ctx, req)
	for err != nil && count < 10 {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error("temporal server error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.TimeoutError:
			l.Error("time out error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.ApplicationError:
			l.Error("temporal custom error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Type() {
			case comwkfl.ERR_SESSION_CTX:
				resp, err = processFileSignal(ctx, resp)
				continue
			case comwkfl.ERR_WRONG_HOST:
				return req, err
			case comwkfl.ERR_MISSING_FILE_NAME:
				return req, err
			case comwkfl.ERR_MISSING_REQSTR:
				return req, err
			case ERR_MISSING_OFFSETS:
				return req, err
			case comwkfl.ERR_QUERY_HANDLER:
				return req, err
			default:
				resp, err = processFileSignal(ctx, resp)
				continue
			}
		case *temporal.PanicError:
			l.Error("temporal panic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		case *temporal.CanceledError:
			l.Error("temporal canceled error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		default:
			l.Error("other error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			resp, err = processFileSignal(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessFileSignalWorkflow - failed",
			slog.String("err-msg", err.Error()),
			slog.Int("tries", count),
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_PROCESS_FILE_WKFL, ERR_PROCESS_FILE_WKFL, errors.WrapError(err, ERR_PROCESS_FILE_WKFL))
	}

	l.Info(
		"ProcessFileSignalWorkflow - completed",
		slog.String("file", req.FileName),
		slog.String("reqstr", req.RequestedBy))
	return resp, nil
}

func processFileSignal(ctx workflow.Context, req *models.CSVInfo) (*models.CSVInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ProcessFileSignalWorkflow - starting execution", slog.String("file", req.FileName))

	// set execution duration
	executionDuration := comwkfl.ONE_DAY

	// // build session context
	// so := &workflow.SessionOptions{
	// 	CreationTimeout:  10 * time.Minute,
	// 	ExecutionTimeout: executionDuration,
	// }
	// sessionCtx, err := workflow.CreateSession(ctx, so)
	// if err != nil {
	// 	l.Error(comwkfl.ERR_SESSION_CTX, slog.Any("error", err))
	// 	return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_SESSION_CTX, comwkfl.ERR_SESSION_CTX, errors.WrapError(err, comwkfl.ERR_SESSION_CTX))
	// }
	// sessionCtx = workflow.WithStartToCloseTimeout(sessionCtx, executionDuration)
	// defer workflow.CompleteSession(sessionCtx)

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (models.CSVInfoState, error) {
		return models.MapCSVInfoToState(req), nil
	}); err != nil {
		l.Info("ProcessFileSignalWorkflow - SetQueryHandler failed", slog.Any("error", err))
		return req, temporal.NewApplicationErrorWithCause(comwkfl.ERR_QUERY_HANDLER, comwkfl.ERR_QUERY_HANDLER, errors.WrapError(err, comwkfl.ERR_QUERY_HANDLER))
	}

	runId := workflow.GetInfo(ctx).WorkflowExecution.RunID
	wkflId := workflow.GetInfo(ctx).WorkflowExecution.ID
	l.Debug("ProcessFileSignalWorkflow - current workflow execution", slog.String("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))

	if runs, err := comwkfl.ExecuteSearchRunActivity(ctx, &models.RunParams{
		Type:        ProcessFileSignalWorkflowName,
		ExternalRef: req.FileName,
	}); err != nil {
		l.Error("ProcessFileSignalWorkflow - no corresponding run state found, creating new run state", slog.Any("error", err), slog.String("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))

		// create a new run
		if runInfo, err := comwkfl.ExecuteCreateRunActivity(ctx, &models.RunParams{
			RunId:       runId,
			WorkflowId:  wkflId,
			RequestedBy: req.RequestedBy,
			ExternalRef: req.FileName,
			Type:        ProcessFileSignalWorkflowName,
		}); err != nil {
			l.Error(comwkfl.ERR_CREATING_WKFL_RUN, slog.Any("error", err), slog.String("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))
			return req, err
		} else {
			l.Info(comwkfl.WKFL_RUN_CREATED, slog.String("run-id", runInfo.RunId), slog.String("wkfl-id", runInfo.WorkflowId), slog.String("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))
			req.RunId = runInfo.RunId
			req.WorkflowId = runInfo.WorkflowId
		}
	} else {
		l.Debug("ProcessFileSignalWorkflow - found existing run state", slog.Any("run", runs[0]), slog.String("file", req.FileName), slog.String("run-id", runs[0].RunId), slog.String("wkfl-id", runs[0].WorkflowId))
		req.RunId = runs[0].RunId
		req.WorkflowId = runs[0].WorkflowId
	}

	l.Debug("ProcessFileSignalWorkflow - starting file download", slog.String("file", req.FileName), slog.String("run-id", req.RunId), slog.String("wkfl-id", req.WorkflowId))
	// download file
	if _, err := fiwkfl.ExecuteDownloadFileActivity(ctx, &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
	}); err != nil {
		l.Error("ProcessFileSignalWorkflow - error dowloading file", slog.String("error", err.Error()), slog.Any("file", req.FileName))
		return req, err
	}
	l.Debug("ProcessFileSignalWorkflow - file downloaded, starting process CSV workflow", slog.Any("file", req.FileName), slog.Any("type", req.Type))

	// build child workflow context
	scwo := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: executionDuration,
	}
	cwCtx := workflow.WithChildOptions(ctx, scwo)

	// start CSV processing workflow to process file
	// TODO - add support for other entity types
	future := workflow.ExecuteChildWorkflow(cwCtx, ProcessCSVWorkflow, req)

	var childWE workflow.Execution
	if err := future.GetChildWorkflowExecution().Get(ctx, &childWE); err == nil {
		l.Debug("ProcessFileSignalWorkflow - ProcessCSVWorkflow started", slog.String("child-wkfl-id", childWE.ID), slog.String("child-wkfl-run-id", childWE.RunID))
		req.ProcessRunId = childWE.RunID
		req.ProcessWorkflowId = childWE.ID
	}

	var resp models.CSVInfo
	err := future.Get(ctx, &resp)
	if err != nil {
		l.Error("ProcessFileSignalWorkflow - error executing ProcessCSVWorkflow child workflow", slog.Any("error", err), slog.String("file", req.FileName))
	} else {
		errCount := 0
		resultCount := 0
		recCount := 0
		for _, v := range resp.Results {
			errCount = errCount + len(v.Errors)
			resultCount = resultCount + len(v.Results)
			recCount = recCount + len(v.Errors) + len(v.Results)
		}
		req.FileSize = resp.FileSize
		req.OffSets = resp.OffSets
		req.Headers = resp.Headers
		req.Results = resp.Results
		req.HostID = resp.HostID

		l.Debug(
			"ProcessFileSignalWorkflow - ProcessCSVWorkflow response",
			slog.Int64("size", resp.FileSize),
			slog.Int("batches", len(resp.OffSets)),
			slog.Int("batches-processed", len(resp.Results)),
			slog.Int("errCount", errCount),
			slog.Int("resultCount", resultCount),
			slog.Int("recCount", recCount))
	}
	l.Debug("ProcessFileSignalWorkflow - file processed, updating run state", slog.Any("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))

	// update run
	if runInfo, err := comwkfl.ExecuteUpdateRunActivity(ctx, &models.RunParams{
		RunId:       runId,
		WorkflowId:  wkflId,
		RequestedBy: req.RequestedBy,
		Status:      string(models.COMPLETED),
	}); err != nil {
		l.Error(comwkfl.ERR_UPDATING_WKFL_RUN, slog.Any("error", err), slog.Any("file", req.FileName), slog.String("run-id", runId), slog.String("wkfl-id", wkflId))
		return &resp, err
	} else {
		l.Debug(comwkfl.WKFL_RUN_UPDATED, slog.String("run-id", runInfo.RunId), slog.String("wkfl-id", runInfo.WorkflowId))
	}

	return &resp, nil
}
