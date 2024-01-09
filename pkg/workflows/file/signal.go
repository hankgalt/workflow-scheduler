package file

import (
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const FILE_SIG_CHAN = "fileSignalChan"

// FileSignalWorkflow workflow processor
func FileSignalWorkflow(ctx workflow.Context) error {
	fileSigCh := workflow.GetSignalChannel(ctx, FILE_SIG_CHAN)
	done := false

	l := workflow.GetLogger(ctx)
	l.Info("FileSignalWorkflow started")

	runId := workflow.GetInfo(ctx).WorkflowExecution.RunID
	wkflId := workflow.GetInfo(ctx).WorkflowExecution.ID

	if runs, err := common.ExecuteSearchRunActivity(ctx, &models.RunParams{
		Type: FileSignalWorkflowName,
	}); err != nil {
		l.Error("error searching workflow runs", zap.Error(err))

		// create a new run
		if runInfo, err := common.ExecuteCreateRunActivity(ctx, &models.RunParams{
			RunId:       runId,
			WorkflowId:  wkflId,
			RequestedBy: FileSignalWorkflowName,
			Type:        FileSignalWorkflowName,
		}); err != nil {
			l.Error(common.ERR_CREATING_WKFL_RUN, zap.Error(err))
			return err
		} else {
			l.Info(common.WKFL_RUN_CREATED, zap.String("run-id", runInfo.RunId), zap.String("wkfl-id", runInfo.WorkflowId))
		}
	} else {
		l.Info("FileSignalWorkflow - found existing run", zap.Any("run", runs[0]))
		runId = runs[0].RunId
		wkflId = runs[0].WorkflowId
	}

	for {
		s := workflow.NewSelector(ctx)
		s.AddReceive(fileSigCh, func(c workflow.Channel, ok bool) {
			if ok {
				var sig models.FileSignal
				c.Receive(ctx, &sig)
				l.Info("FileSignalWorkflow - received file signal", zap.Any("signal", sig))
				done = sig.Done
			}
		})

		s.AddReceive(ctx.Done(), func(c workflow.Channel, ok bool) {
			l.Info("FileSignalWorkflow - context done.", zap.String("ctx-err", ctx.Err().Error()))
			fileSigCh.Close()
		})

		s.Select(ctx)

		if done {
			l.Info("Done signal received")
			break
		}
	}

	l.Info("FileSignalWorkflow completed")

	// update run
	if runInfo, err := common.ExecuteUpdateRunActivity(ctx, &models.RunParams{
		RunId:       runId,
		WorkflowId:  wkflId,
		RequestedBy: FileSignalWorkflowName,
		Status:      string(models.COMPLETED),
	}); err != nil {
		l.Error(common.ERR_UPDATING_WKFL_RUN, zap.Error(err))
		return err
	} else {
		l.Info(common.WKFL_RUN_UPDATED, zap.String("run-id", runInfo.RunId), zap.String("wkfl-id", runInfo.WorkflowId))
	}
	return nil
}
