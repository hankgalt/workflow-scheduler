package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/comfforts/errors"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/zap"

	"github.com/comfforts/localstorage"

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
		Org:         req.Org,
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
		Org:         req.Org,
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
		Org:         req.Org,
		HostID:      hostId,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	l.Info("DeleteFileActivity - succeed", zap.String("filePath", req.FileName))
	return acResp, nil
}

func ReadCSVActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("ReadCSVActivity - started", zap.Any("req", req))

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	_, err := os.Stat(localFilePath)
	if err != nil {
		logger.Error("ReadCSVActivity - file doesn't exist",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("ReadCSVActivity - file doesn't exist")
	}

	lsCl, err := localstorage.NewLocalStorageClient(logger)
	if err != nil {
		logger.Error("ReadCSVActivity - local storage client error", zap.Error(err))
		return nil, err
	}

	resp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}

	resCh := make(chan []string)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = lsCl.ReadCSVFile(ctx, localFilePath, resCh, errCh)
	if err != nil {
		logger.Error("ReadCSVActivity - error reading file", zap.Error(err))
		cancel()
		return nil, err
	}

	errs := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			logger.Info("ReadCSVActivity - file processing done, context closed", zap.Int("resultCount", resp.Count), zap.Int("errCount", len(errs)))
			return resp, nil
		case r, ok := <-resCh:
			if !ok {
				logger.Info("ReadCSVActivity - result stream closed", zap.Int("resultCount", resp.Count), zap.Int("errCount", len(errs)))
				return resp, nil
			} else {
				if r != nil {
					logger.Info("ReadCSVActivity - result", zap.Any("result", r))
					resp.Count++
				}
			}
		case err, ok := <-errCh:
			if !ok {
				logger.Info("ReadCSVActivity - error stream closed", zap.Int("resultCount", resp.Count), zap.Int("errCount", len(errs)))
				return resp, nil
			} else {
				if err != nil {
					logger.Error("ReadCSVActivity - record read error", zap.Error(err))
					errs[err.Error()]++
				}
			}
		}
	}
}

func CreateFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("createFileActivity - started", zap.Any("req", req))

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}

	localFilePath := filepath.Join(dataPath, req.FileName)
	_, err := os.Stat(localFilePath)
	if err != nil {
		logger.Info("createFileActivity - file doesn't exist, will create a file with test data", zap.Error(err))
		localFilePath, err = CreateJSONFile(filepath.Dir(localFilePath), filepath.Base(localFilePath))
		if err != nil {
			logger.Error("error creating file", zap.Error(err), zap.String("localFilePath", localFilePath))
			return nil, err
		}
	} else {
		logger.Info("createFileActivity - file already exists, will return existing file's info", zap.Error(err))
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
		HostID:      HostID,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}
	logger.Info("createFileActivity - succeed", zap.String("filePath", req.FileName), zap.String("hostId", HostID))
	return acResp, nil
}

func DryRunActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("dryRunActivity - started", zap.Any("req", req))

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if req.HostID != HostID {
		logger.Error("dryRunActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("dryRunActivity - running on wrong host")
	}

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	_, err := os.Stat(localFilePath)
	if err != nil {
		logger.Error("dryRunActivity - file doesn't exist",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("dryRunActivity - file doesn't exist")
	}

	ll, err := localstorage.NewLocalStorageClient(logger)
	if err != nil {
		logger.Error("dryRunActivity - local storage client error", zap.Error(err))
		return nil, err
	}

	acResp := &models.RequestInfo{
		FileName:    req.FileName,
		RequestedBy: req.RequestedBy,
		Org:         req.Org,
		HostID:      HostID,
		RunId:       req.RunId,
		WorkflowId:  req.WorkflowId,
		Status:      req.Status,
	}

	resCh := make(chan models.JSONMapper)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = ll.ReadJSONFile(ctx, localFilePath, resCh, errCh)
	if err != nil {
		logger.Error("dryRunActivity - error reading file", zap.Error(err))
		close(resCh)
		close(errCh)
		cancel()
		return nil, err
	}

	errs := map[string]int{}
	resCount := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("dryRunActivity - file processing done, context closed", zap.Int("resultCount", resCount), zap.Int("errCount", len(errs)))
			acResp.Count = resCount
			return acResp, nil
		case r, ok := <-resCh:
			if !ok {
				logger.Info("dryRunActivity - result stream closed", zap.Int("resultCount", resCount), zap.Int("errCount", len(errs)))
				acResp.Count = resCount
				return acResp, nil
			} else {
				if r != nil {
					logger.Info("dryRunActivity - result", zap.Any("result", r))
					resCount++
				}
			}
		case err, ok := <-errCh:
			if !ok {
				logger.Info("dryRunActivity - error stream closed", zap.Int("resultCount", resCount), zap.Int("errCount", len(errs)))
				acResp.Count = resCount
				return acResp, nil
			} else {
				if err != nil {
					logger.Error("dryRunActivity - record read error", zap.Error(err))
					errs[err.Error()]++
				}
			}
		}
	}
}

func SaveFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("SaveFileActivity - started", zap.String("fileName", req.FileName))

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if req.HostID != HostID {
		logger.Error("SaveFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("SaveFileActivity - running on wrong host")
	}

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)
	dataFilePath := filepath.Join(filepath.Dir(localFilePath), "data.json")
	_, err := os.Stat(dataFilePath)
	if err != nil {
		logger.Error("SaveFileActivity - file doesn't exist",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID),
			zap.String("dataFilePath", dataFilePath))
		return nil, errors.NewAppError("SaveFileActivity - file doesn't exist")
	}

	data, err := os.ReadFile(dataFilePath)
	if err != nil {
		logger.Error("SaveFileActivity - error reading file",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID),
			zap.String("TargetDataFile", dataFilePath))
		return nil, errors.NewAppError("processFileActivity - error reading file")
	}

	var fileData models.JSONMapper
	err = json.Unmarshal(data, &fileData)
	if err != nil {
		logger.Error("SaveFileActivity - error reading file data",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID),
			zap.String("TargetDataFile", dataFilePath))
		return nil, errors.NewAppError("SaveFileActivity - error reading file data")
	}

	req.HostID = HostID
	logger.Info("SaveFileActivity succeed.", zap.String("filePath", req.FileName))
	return req, nil
}

func DeleteLocalFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("DeleteLocalFileActivity - started", zap.String("FileName", req.FileName))

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if req.HostID != HostID {
		logger.Error("DeleteLocalFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("DeleteLocalFileActivity - running on wrong host")
	}

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	err := os.Remove(localFilePath)
	if err != nil {
		logger.Error("DeleteLocalFileActivity - error deleting source file",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("DeleteLocalFileActivity - error deleting source file")
	}

	req.HostID = HostID
	logger.Info("DeleteLocalFileActivity succeed.", zap.String("filePath", req.FileName))
	return req, nil
}
