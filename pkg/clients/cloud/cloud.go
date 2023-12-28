package cloud

import (
	"context"
	"os"
	"path/filepath"

	"github.com/comfforts/cloudstorage"
	"github.com/comfforts/errors"
	"go.uber.org/zap"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
)

const CloudClientContextKey = clients.ContextKey("cloud-client")
const CloudBucketContextKey = clients.ContextKey("cloud-bucket")

const (
	ERR_MISSING_REQUIRED = "error: missing required configuration"
	ERR_CLOUD_CLOSE      = "error closing cloud connection"
	ERROR_CREATING_FILE  = "creating file - %s"
	ERROR_NO_FILE        = "%s doesn't exist"
)

var (
	ErrMissingRequired = errors.NewAppError(ERR_MISSING_REQUIRED)
)

type Client interface {
	DownloadFile(ctx context.Context, filePath, bucket string) error
	UploadFile(ctx context.Context, filePath, bucket string) error
	DeleteFile(ctx context.Context, filePath, bucket string) error
	Close() error
}

type gcpCloudClient struct {
	cloudstorage.CloudStorage
	logger *zap.Logger
	cfg    *CloudConfig
}

type CloudConfig struct {
	CredsPath string
	DataPath  string
}

func NewCloudConfig(credsPath, dataPath string) (*CloudConfig, error) {
	if credsPath == "" {
		credsPath = os.Getenv("CREDS_PATH")
	}

	if dataPath == "" {
		dataPath = os.Getenv("DATA_PATH")
	}

	if dataPath == "" {
		dataPath = "data"
	}

	if credsPath == "" {
		return nil, ErrMissingRequired
	}

	return &CloudConfig{
		CredsPath: credsPath,
		DataPath:  dataPath,
	}, nil
}

func NewGCPCloudClient(cfg *CloudConfig, logger *zap.Logger) (*gcpCloudClient, error) {
	cscCfg := cloudstorage.CloudStorageClientConfig{
		CredsPath: cfg.CredsPath,
	}
	csc, err := cloudstorage.NewCloudStorageClient(cscCfg, logger)
	if err != nil {
		logger.Error("error creating cloud storage client", zap.Error(err))
		return nil, err
	}

	return &gcpCloudClient{
		cfg:          cfg,
		CloudStorage: csc,
		logger:       logger,
	}, nil
}

func (cl *gcpCloudClient) DownloadFile(ctx context.Context, filePath, bucket string) error {
	var fmod int64
	localFilePath := filepath.Join(cl.cfg.DataPath, filePath)
	err := os.MkdirAll(filepath.Dir(localFilePath), os.ModePerm)
	if err != nil {
		cl.logger.Error("error creating local file", zap.Error(err), zap.String("localFilePath", localFilePath))
		return err
	}
	f, err := os.Create(localFilePath)
	if err != nil {
		cl.logger.Error("error creating local file", zap.Error(err), zap.String("filepath", localFilePath))
		return err
	}

	defer func() {
		if err := f.Close(); err != nil {
			cl.logger.Error("error closing file", zap.Error(err), zap.String("filepath", filePath))
		}
	}()

	cfr, err := cloudstorage.NewCloudFileRequest(bucket, filepath.Base(filePath), filepath.Dir(filePath), fmod)
	if err != nil {
		cl.logger.Error("error creating cloud file request", zap.Error(err), zap.String("filepath", filePath))
		return err
	}

	n, err := cl.CloudStorage.DownloadFile(ctx, f, cfr)
	if err != nil {
		cl.logger.Error("error downloading file", zap.Error(err), zap.String("filepath", filePath))
		return err
	}
	cl.logger.Info(
		"downloaded file",
		zap.String("data-dir", cl.cfg.DataPath),
		zap.String("rm-dir", filepath.Dir(filePath)),
		zap.String("rm-file", filepath.Base(filePath)),
		zap.String("local-file", localFilePath),
		zap.Int64("bytes", n))
	return nil
}

func (cl *gcpCloudClient) UploadFile(ctx context.Context, filePath, bucket string) error {
	localFilePath := filepath.Join(cl.cfg.DataPath, filePath)
	fStats, err := os.Stat(localFilePath)
	if err != nil {
		cl.logger.Error("error accessing file", zap.Error(err), zap.String("localFilePath", localFilePath))
		return err
	}
	fmod := fStats.ModTime().Unix()
	cl.logger.Info("file mod time", zap.Int64("modtime", fmod), zap.String("localFilePath", localFilePath))

	file, err := os.Open(localFilePath)
	if err != nil {
		cl.logger.Error("error accessing file", zap.Error(err))
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			cl.logger.Error("error closing file", zap.Error(err), zap.String("localFilePath", localFilePath))
		}
	}()

	cfr, err := cloudstorage.NewCloudFileRequest(bucket, filepath.Base(filePath), filepath.Dir(filePath), fmod)
	if err != nil {
		cl.logger.Error("error creating request", zap.Error(err), zap.String("filepath", filePath))
		return err
	}

	n, err := cl.CloudStorage.UploadFile(ctx, file, cfr)
	if err != nil {
		cl.logger.Error("error uploading file", zap.Error(err))
		return err
	}
	cl.logger.Info(
		"uploaded file",
		zap.String("data-dir", cl.cfg.DataPath),
		zap.String("rm-dir", filepath.Dir(filePath)),
		zap.String("rm-file", filepath.Base(filePath)),
		zap.String("local-file", localFilePath),
		zap.Int64("bytes", n))
	return nil
}

func (cl *gcpCloudClient) DeleteFile(ctx context.Context, filePath, bucket string) error {
	cfr, err := cloudstorage.NewCloudFileRequest(bucket, filepath.Base(filePath), filepath.Dir(filePath), 0)
	if err != nil {
		cl.logger.Error("error creating delete request", zap.Error(err), zap.String("filepath", filePath))
		return err
	}

	err = cl.CloudStorage.DeleteObject(ctx, cfr)
	if err != nil {
		cl.logger.Error("error deleting file", zap.Error(err))
		return err
	}
	cl.logger.Info(
		"file deleted",
		zap.String("file", filepath.Base(filePath)),
		zap.String("path", filepath.Dir(filePath)))
	return nil
}

func (cl *gcpCloudClient) Close() error {
	err := cl.CloudStorage.Close()
	if err != nil {
		cl.logger.Error(ERR_CLOUD_CLOSE, zap.Error(err))
		return err
	}
	return nil
}
