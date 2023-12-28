package file

import (
	"fmt"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

// UploadFileWorkflow workflow processer
func UploadFileWorkflow(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := workflow.GetLogger(ctx)
	l.Info(
		"UploadFileWorkflow started",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))

	count := 0
	configErr := false
	resp, err := uploadFile(ctx, req)
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
				resp, err = uploadFile(ctx, resp)
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
			case common.ERR_MISSING_FILE:
				configErr = true
				return req, err
			case common.ERR_MISSING_SCHEDULER_CLIENT:
				configErr = true
				return req, err
			default:
				resp, err = uploadFile(ctx, resp)
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
			resp, err = uploadFile(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"UploadFileWorkflow failed",
			zap.String("err-msg", err.Error()),
			zap.Int("tries", count),
			zap.Bool("config-err", configErr),
		)
		return resp, cadence.NewCustomError("UploadFileWorkflow failed", err)
	}

	l.Info(
		"UploadFileWorkflow completed",
		zap.String("file", req.FileName),
		zap.String("reqstr", req.RequestedBy))
	return resp, nil
}

func uploadFile(ctx workflow.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := workflow.GetLogger(ctx)

	// set execution duration
	executionDuration := 5 * time.Minute

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

	// setup workflow & run references
	req.RunId = workflow.GetInfo(ctx).WorkflowExecution.RunID
	req.WorkflowId = workflow.GetInfo(ctx).WorkflowExecution.ID

	// get csv header
	req, err = ExecuteUploadFileActivity(sessionCtx, req)
	if err != nil {
		l.Error("UploadFileWorkflow - error uploading file", zap.String("error", err.Error()))
		return req, err
	}
	l.Info("UploadFileWorkflow file uploaded", zap.Any("file", req.FileName))
	return req, nil
}
