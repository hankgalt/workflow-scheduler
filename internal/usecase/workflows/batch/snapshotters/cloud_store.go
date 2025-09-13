package snapshotters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const CloudSnapshotter = "cloud-snapshotter"

const ERR_MISSING_CLOUD_CREDENTIALS = "cloud csv: missing credentials path"

var ErrMissingCloudCredentials = errors.New(ERR_MISSING_CLOUD_CREDENTIALS)

type cloudSnapshotter struct {
	bucket string
	path   string
	client *storage.Client
}

// Name of the snapshotter.
func (s cloudSnapshotter) Name() string { return CloudSnapshotter }

// Close closes the cloud snapshotter.
func (s cloudSnapshotter) Close(ctx context.Context) error {
	return s.client.Close()
}

func (s cloudSnapshotter) Snapshot(ctx context.Context, key string, snapshot any) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("cloudSnapshotter:Snapshot - error getting logger from context: %w", err)
	}

	fPath := filepath.Join(s.path, key+".json")

	// check for cloud object existence
	obj := s.client.Bucket(s.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		l.Debug("cloud file doesn't exist, will create new file", "filepath", fPath)
	} else {
		l.Debug("cloud file exists, will create new version", "created-at", attrs.Created.Unix(), "updated-at", attrs.Updated.Unix(), "filepath", fPath)
	}

	// create cloud object writer
	wc := obj.NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			l.Error("error closing cloud file", "filepath", fPath, "error", err.Error())
		}
	}()

	snapshotBytes, ok := snapshot.([]byte)
	if !ok {
		return ErrInvalidSnapshotFormat
	}
	file := bytes.NewReader(snapshotBytes)

	// copy file content to cloud object
	nBytes, err := io.Copy(wc, file)
	if err != nil {
		return fmt.Errorf("error uploading file %s: %w", fPath, err)
	}
	l.Debug("cloud file created/updated", "filepath", fPath, "bytes", nBytes)
	return nil
}

type CloudSnapshotterConfig struct {
	Bucket string
	Path   string
}

// Name of the snapshotter.
func (s CloudSnapshotterConfig) Name() string { return CloudSnapshotter }

func (s CloudSnapshotterConfig) BuildSnapshotter(ctx context.Context) (domain.Snapshotter, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("CloudSnapshotterConfig:BuildSnapshotter - error getting logger from context: %w", err)
	}

	// Ensure the environment variable is set for GCP credentials
	cPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if cPath == "" {
		return nil, ErrMissingCloudCredentials
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		l.Error("cloud storage: failed to create client", "error", err.Error())
		return nil, err
	}

	return &cloudSnapshotter{
		bucket: s.Bucket,
		path:   s.Path,
		client: client,
	}, nil
}
