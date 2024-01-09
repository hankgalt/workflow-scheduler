package file

import (
	"context"

	"github.com/comfforts/errors"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

/**
 * activities used by file processing workflow.
 */
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

const DataPathContextKey = clients.ContextKey("data-path")

type FileActivityFn = func(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error)

func DownloadFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	l.Info("downloadFileActivity - started", zap.Any("req", req))

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
		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return req, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		l.Error(ERR_MISSING_CLOUD_CLIENT)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_CLIENT, ErrMissingCloudClient)
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	}

	l.Info("downloadFileActivity - downloading file", zap.String("filePath", req.FileName))

	err := cloudClient.DownloadFile(ctx, req.FileName, bucket)
	if err != nil {
		l.Error(ERR_FILE_DOWNLOAD, zap.Error(err))
		return req, cadence.NewCustomError(ERR_FILE_DOWNLOAD, err)
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		HostID:      hostId,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	l.Info("downloadFileActivity - done", zap.String("filePath", req.FileName))
	return acResp, nil
}

func UploadFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	l.Info("uploadFileActivity - started", zap.Any("req", req))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if hostId != HostID {
		l.Error("uploadFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", hostId))
		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return req, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		l.Error(ERR_MISSING_CLOUD_CLIENT)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_CLIENT, ErrMissingCloudClient)
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	}

	err := cloudClient.UploadFile(ctx, req.FileName, bucket)
	if err != nil {
		l.Error(ERR_FILE_UPLOAD, zap.Error(err))
		return nil, cadence.NewCustomError(ERR_FILE_UPLOAD, err)
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		HostID:      hostId,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	l.Info("uploadFileActivity - succeed", zap.String("filePath", req.FileName))
	return acResp, nil
}

func DeleteFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	l := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	l.Info("DeleteFileActivity - started", zap.Any("req", req))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if hostId != HostID {
		l.Error("DeleteFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", hostId))
		return req, cadence.NewCustomError(common.ERR_WRONG_HOST, common.ErrWrongHost)
	}

	if req.FileName == "" {
		l.Error(common.ERR_MISSING_FILE_NAME)
		return req, cadence.NewCustomError(common.ERR_MISSING_FILE_NAME, common.ErrMissingFileName)
	}

	if req.RequestedBy == "" {
		l.Error(common.ERR_MISSING_REQSTR)
		return req, cadence.NewCustomError(common.ERR_MISSING_REQSTR, common.ErrMissingReqstr)
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		l.Error(ERR_MISSING_CLOUD_CLIENT)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_CLIENT, ErrMissingCloudClient)
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, cadence.NewCustomError(ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	}

	err := cloudClient.DeleteFile(ctx, req.FileName, bucket)
	if err != nil {
		l.Error(ERR_FILE_DELETE, zap.Error(err))
		return nil, cadence.NewCustomError(ERR_FILE_DELETE, err)
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		HostID:      hostId,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	l.Info("DeleteFileActivity - succeed", zap.String("filePath", req.FileName))
	return acResp, nil
}
