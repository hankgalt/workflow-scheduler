package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/comfforts/errors"
	"go.uber.org/cadence/activity"
	"go.uber.org/zap"

	"github.com/comfforts/localstorage"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

/**
 * activities used by file processing workflow.
 */
const (
	CreateFileActivityName   = "CreateFileActivity"
	UploadFileActivityName   = "UploadFileActivity"
	DownloadFileActivityName = "DownloadFileActivity"
	DryRunActivityName       = "DryRunActivity"
	SaveFileActivityName     = "SaveFileActivity"
	DeleteFileActivityName   = "DeleteFileActivity"
	ReadCSVActivityName      = "ReadCSVActivity"
)

const DataPathContextKey = clients.ContextKey("data-path")

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

func UploadFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("uploadFileActivity - started", zap.Any("req", req))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if hostId != HostID {
		logger.Error("uploadFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", hostId))
		return nil, errors.NewAppError("uploadFileActivity - running on wrong host")
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		logger.Error("uploadFileActivity - workflow context missing cloud client")
		return nil, errors.NewAppError("workflow context missing cloud client")
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		logger.Error("uploadFileActivity - workflow context missing cloud bucket name")
		return nil, errors.NewAppError("workflow context missing cloud bucket name")
	}

	err := cloudClient.UploadFile(ctx, req.FileName, bucket)
	if err != nil {
		logger.Error("uploadFileActivity - error uploading file", zap.Error(err))
		return nil, err
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
	logger.Info("uploadFileActivity - succeed", zap.String("filePath", req.FileName))
	return acResp, nil
}

func DownloadFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("downloadFileActivity - started", zap.Any("req", req))

	hostId := req.HostID
	if hostId == "" {
		hostId = HostID
	}

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if hostId != HostID {
		logger.Error("downloadFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", hostId),
		)
		return nil, errors.NewAppError("downloadFileActivity - running on wrong host")
	}

	cloudClient := ctx.Value(cloud.CloudClientContextKey).(cloud.Client)
	if cloudClient == nil {
		logger.Error("downloadFileActivity - workflow context missing cloud client")
		return nil, errors.NewAppError("workflow context missing cloud client")
	}

	bucket := ctx.Value(cloud.CloudBucketContextKey).(string)
	if bucket == "" {
		logger.Error("downloadFileActivity - workflow context missing cloud bucket name")
		return nil, errors.NewAppError("workflow context missing cloud bucket name")
	}

	err := cloudClient.DownloadFile(ctx, req.FileName, bucket)
	if err != nil {
		logger.Error("downloadFileActivity - error downloading file", zap.Error(err))
		return nil, err
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
	logger.Info("downloadFileActivity - done", zap.String("filePath", req.FileName))
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

func DeleteFileActivity(ctx context.Context, req *models.RequestInfo) (*models.RequestInfo, error) {
	logger := activity.GetLogger(ctx).With(zap.String("HostID", HostID))
	logger.Info("DeleteFileActivity - started", zap.String("FileName", req.FileName))

	// assert that we are running on the same host as the file was downloaded
	// this check is not necessary, just to demo the host specific tasklist is working
	if req.HostID != HostID {
		logger.Error("DeleteFileActivity - running on wrong host",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("DeleteFileActivity - running on wrong host")
	}

	dataPath := ctx.Value(DataPathContextKey).(string)
	if dataPath == "" {
		dataPath = "data"
	}
	localFilePath := filepath.Join(dataPath, req.FileName)

	err := os.Remove(localFilePath)
	if err != nil {
		logger.Error("DeleteFileActivity - error deleting source file",
			zap.String("TargetFile", req.FileName),
			zap.String("TargetHostID", req.HostID))
		return nil, errors.NewAppError("DeleteFileActivity - error deleting source file")
	}

	req.HostID = HostID
	logger.Info("DeleteFileActivity succeed.", zap.String("filePath", req.FileName))
	return req, nil
}
