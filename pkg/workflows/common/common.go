package common

import (
	"time"

	"github.com/comfforts/errors"
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
	ERR_MISSING_SCHEDULER_CLIENT = "workflow context missing scheduler grpc client"
)

var (
	ErrSessionCtx       = errors.NewAppError(ERR_SESSION_CTX)
	ErrMissingSchClient = errors.NewAppError(ERR_MISSING_SCHEDULER_CLIENT)
)
