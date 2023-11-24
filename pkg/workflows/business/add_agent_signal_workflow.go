package business

import (
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	AGENT_SIG_CHAN = "agentSigChan"
	AGENT_ERR_CHAN = "agentErrChan"
)

// AddAgentSignalWorkflow workflow decider
func AddAgentSignalWorkflow(ctx workflow.Context, req *models.BatchInfo) (*models.BatchInfo, error) {
	var drainedAllSignals bool
	signalsPerExecution := 0
	resCount := req.ProcessedCount
	errCount := req.ErrCount

	logger := workflow.GetLogger(ctx)
	logger.Info("AddAgentSignalWorkflow started")

	for {
		s := workflow.NewSelector(ctx)
		s.AddReceive(workflow.GetSignalChannel(ctx, AGENT_SIG_CHAN), func(c workflow.Channel, ok bool) {
			if ok {
				var fields models.CSVRecord
				c.Receive(ctx, &fields)
				resCount++
				signalsPerExecution++
				logger.Info("AddAgentSignalWorkflow - received signal on result channel.", zap.Any("fields", fields), zap.Int("result-count", resCount))
			}
		})
		s.AddReceive(workflow.GetSignalChannel(ctx, AGENT_ERR_CHAN), func(c workflow.Channel, ok bool) {
			if ok {
				var err error
				c.Receive(ctx, &err)
				errCount++
				signalsPerExecution++
				logger.Info("AddAgentSignalWorkflow - received signal on error channel.", zap.Error(err), zap.Int("err-count", errCount))
			}
		})

		if signalsPerExecution >= req.BatchCount {
			s.AddDefault(func() {
				// this indicate that we have drained all signals within the decision task, and it's safe to do a continueAsNew
				drainedAllSignals = true
				logger.Info("AddAgentSignalWorkflow - processed add agent signal batch")
			})
		}

		s.Select(ctx)

		if drainedAllSignals {
			logger.Info("AddAgentSignalWorkflow - completed batch, returning batch info")
			batchInfo := &models.BatchInfo{
				BatchCount:     req.BatchCount,
				ProcessedCount: resCount,
				ErrCount:       errCount,
			}
			return batchInfo, nil
		}
	}
}
