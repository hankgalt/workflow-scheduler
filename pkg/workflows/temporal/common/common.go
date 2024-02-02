package common

import (
	"time"

	"github.com/comfforts/errors"
	"go.temporal.io/sdk/temporal"
)

const (
	FIVE_MINS   = 5 * time.Minute
	THIRTY_MINS = 30 * time.Minute
	ONE_HOUR    = time.Hour
	ONE_DAY     = 24 * time.Hour
)

const (
	ERR_SESSION_CTX              = "error getting session context"
	ERR_UNHANDLED                = "unhandled error"
	ERR_CREATING_WKFL_RUN        = "error creating workflow run record"
	WKFL_RUN_CREATED             = "workflow run record created"
	ERR_UPDATING_WKFL_RUN        = "error updating workflow run record"
	WKFL_RUN_UPDATED             = "workflow run record updated"
	ERR_MISSING_FILE_NAME        = "error missing file name"
	ERR_MISSING_FILE             = "error missing file"
	ERR_MISSING_REQSTR           = "error missing requester"
	ERR_WRONG_HOST               = "error running on wrong host"
	ERR_MISSING_SCHEDULER_CLIENT = "workflow context missing scheduler grpc client"
	ERR_SCH_CLIENT_INIT          = "error initializing scheduler client"
	ERR_SCH_CLIENT_CLOSE         = "error closing scheduler client"
	ERR_QUERY_HANDLER            = "error setiing up state query handler"
	CREATE_RUN_ACT_STARTED       = "create run activity started."
	ERR_CREATING_RUN             = "error creating workflow run"
	ERR_SEARCH_RUN               = "error searching workflow run"
	CREATE_RUN_ACT_COMPL         = "create run activity completed."
	UPDATE_RUN_ACT_STARTED       = "update run activity started."
	ERR_UPDATING_RUN             = "error updating workflow run"
	UPDATE_RUN_ACT_COMPL         = "update run activity completed."
)

var (
	ErrSessionCtx       = errors.NewAppError(ERR_SESSION_CTX)
	ErrMissingSchClient = errors.NewAppError(ERR_MISSING_SCHEDULER_CLIENT)
	ErrMissingFileName  = errors.NewAppError(ERR_MISSING_FILE_NAME)
	ErrMissingFile      = errors.NewAppError(ERR_MISSING_FILE)
	ErrMissingReqstr    = errors.NewAppError(ERR_MISSING_REQSTR)
	ErrWrongHost        = errors.NewAppError(ERR_WRONG_HOST)
	ErrSchClientInit    = errors.NewAppError(ERR_SCH_CLIENT_INIT)
	ErrSchClientClose   = errors.NewAppError(ERR_SCH_CLIENT_CLOSE)
	ErrCreatingRun      = errors.NewAppError(ERR_CREATING_RUN)
	ErrUpdatingRun      = errors.NewAppError(ERR_UPDATING_RUN)
	ErrSearchRun        = errors.NewAppError(ERR_SEARCH_RUN)
)

var (
	ErrorMissingSchedulerClient = temporal.NewApplicationErrorWithCause(ERR_MISSING_SCHEDULER_CLIENT, ERR_MISSING_SCHEDULER_CLIENT, ErrMissingSchClient)
	ErrorWrongHost              = temporal.NewApplicationErrorWithCause(ERR_WRONG_HOST, ERR_WRONG_HOST, ErrWrongHost)
	ErrorMissingFileName        = temporal.NewApplicationErrorWithCause(ERR_MISSING_FILE_NAME, ERR_MISSING_FILE_NAME, ErrMissingFileName)
	ErrorMissingReqstr          = temporal.NewApplicationErrorWithCause(ERR_MISSING_REQSTR, ERR_MISSING_REQSTR, ErrMissingReqstr)
)
