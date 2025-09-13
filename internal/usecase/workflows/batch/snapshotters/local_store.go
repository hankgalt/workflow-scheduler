package snapshotters

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const LocalSnapshotter = "local-snapshotter"

const INVALID_SNAPSHOT_FORMAT = "invalid snapshot format"

var ErrInvalidSnapshotFormat = errors.New(INVALID_SNAPSHOT_FORMAT)

type localSnapshotter struct {
	path string
}

// Name of the snapshotter.
func (s localSnapshotter) Name() string { return LocalSnapshotter }

// Close closes the local file snapshotter.
func (s localSnapshotter) Close(ctx context.Context) error {
	// No resources to close for local file snapshotter
	return nil
}

func (s localSnapshotter) Snapshot(ctx context.Context, key string, snapshot any) error {
	// get current dir path
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	fp := filepath.Join(dir, s.path, key+".json")
	snapshotBytes, ok := snapshot.([]byte)
	if !ok {
		return ErrInvalidSnapshotFormat
	}

	return os.WriteFile(fp, append(snapshotBytes, '\n'), 0o644)
}

type LocalSnapshotterConfig struct {
	Path string
}

// Name of the snapshotter.
func (s LocalSnapshotterConfig) Name() string { return LocalSnapshotter }

func (s LocalSnapshotterConfig) BuildSnapshotter(ctx context.Context) (domain.Snapshotter, error) {
	return &localSnapshotter{
		path: s.Path,
	}, nil
}
