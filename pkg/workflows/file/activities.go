package file

import (
	"context"
	"log/slog"

	"github.com/comfforts/errors"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

const (
	DownloadFileActivityName = "DownloadFileActivity"
	UploadFileActivityName   = "UploadFileActivity"
	DeleteFileActivityName   = "DeleteFileActivity"
)

const (
	ERR_MISSING_CLOUD_CLIENT = "error missing cloud client"
	ERR_CLOUD_CFG_INIT       = "error initializing cloud config"
	ERR_CLOUD_CLIENT_INIT    = "error initializing cloud client"
	ERR_CLOUD_CLIENT_CLOSE   = "error closing cloud client"
	ERR_MISSING_CLOUD_BUCKET = "error missing cloud bucket"
	ERR_MISSING_CLOUD_CRED   = "error missing cloud credentials"
	ERR_FILE_DOWNLOAD        = "error file download"
	ERR_FILE_UPLOAD          = "error file upload"
	ERR_FILE_DELETE          = "error file delete"
)

var (
	ErrMissingCloudClient = errors.NewAppError(ERR_MISSING_CLOUD_CLIENT)
	ErrCloudCfgInit       = errors.NewAppError(ERR_CLOUD_CFG_INIT)
	ErrCloudClientInit    = errors.NewAppError(ERR_CLOUD_CLIENT_INIT)
	ErrCloudClientClose   = errors.NewAppError(ERR_CLOUD_CLIENT_CLOSE)
	ErrMissingCloudBucket = errors.NewAppError(ERR_MISSING_CLOUD_BUCKET)
	ErrMissingCloudCred   = errors.NewAppError(ERR_MISSING_CLOUD_CRED)
	ErrFileDownload       = errors.NewAppError(ERR_FILE_DOWNLOAD)
	ErrFileUpload         = errors.NewAppError(ERR_FILE_UPLOAD)
	ErrFileDelete         = errors.NewAppError(ERR_FILE_DELETE)
)

var (
	ErrorMissingCloudBucket = temporal.NewApplicationErrorWithCause(ERR_MISSING_CLOUD_BUCKET, ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	ErrorMissingCloudClient = temporal.NewApplicationErrorWithCause(ERR_MISSING_CLOUD_CLIENT, ERR_MISSING_CLOUD_CLIENT, ErrMissingCloudClient)
)

const DataPathContextKey = clients.ContextKey("data-path")

type FileActivityFn = func(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error)

func DownloadFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := activity.GetLogger(ctx)
	l.Info("downloadFileActivity - started", slog.Any("req", req))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if hostId != HostID {
		l.Error("downloadFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", hostId),
		)
		return req, comwkfl.ErrorWrongHost
	}

	if req.FileName == "" {
		l.Error(comwkfl.ERR_MISSING_FILE_NAME)
		return req, comwkfl.ErrorMissingFileName
	}

	if req.RequestedBy == "" {
		l.Error(comwkfl.ERR_MISSING_REQSTR)
		return req, comwkfl.ErrorMissingReqstr
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		l.Error(ERR_MISSING_CLOUD_CLIENT)
		return req, ErrorMissingCloudClient
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, ErrorMissingCloudBucket
	}

	l.Info("downloadFileActivity - downloading file", slog.String("filePath", req.FileName))

	err := cloudClient.DownloadFile(ctx, req.FileName, bucket)
	if err != nil {
		l.Error(ERR_FILE_DOWNLOAD, slog.Any("errore", err))
		return req, temporal.NewApplicationErrorWithCause(ERR_FILE_DOWNLOAD, ERR_FILE_DOWNLOAD, errors.WrapError(err, ERR_FILE_DOWNLOAD))
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		HostID:      hostId,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	l.Info("downloadFileActivity - done", slog.String("filePath", req.FileName))
	return acResp, nil
}
