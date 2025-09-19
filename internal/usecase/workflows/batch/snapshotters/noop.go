package snapshotters

import (
	"context"
	"errors"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const NoopSnapshotter = "noop-snapshotter"

type noopSnapshotter struct{}

// Name of the snapshotter.
func (s *noopSnapshotter) Name() string { return NoopSnapshotter }

// Close closes the noop snapshotter.
func (s *noopSnapshotter) Close(ctx context.Context) error {
	// No resources to close for noop snapshotter
	return nil
}

func (s *noopSnapshotter) Snapshot(ctx context.Context, key string, snapshot any) error {
	// No-op snapshotter does not create snapshots
	return errors.New("noop snapshotter does not support snapshotting")
}

type NoopSnapshotterConfig struct{}

// Name of the snapshotter.
func (s *NoopSnapshotterConfig) Name() string { return NoopSnapshotter }

func (s *NoopSnapshotterConfig) BuildSnapshotter(ctx context.Context) (domain.Snapshotter, error) {
	return &noopSnapshotter{}, nil
}
