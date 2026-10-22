// Package syncer decides, on every cycle, whether Redis needs a new snapshot and how to get it
// with the least work for the API.
package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"nt-sync/internal/model"
)

// lockTTL covers a slow snapshot download plus the Redis write.
const lockTTL = 10 * time.Minute

// Source is the API.
type Source interface {
	Version(ctx context.Context) (model.VersionInfo, error)
	Snapshot(ctx context.Context) (model.Snapshot, error)
}

// Store keeps the last snapshot locally.
type Store interface {
	Version(ctx context.Context) (string, bool, error)
	Load(ctx context.Context) (model.Snapshot, bool, error)
	Save(ctx context.Context, snap model.Snapshot) error
}

// Publisher is Redis.
type Publisher interface {
	Current(ctx context.Context) (string, error)
	Publish(ctx context.Context, snap model.Snapshot, rows []model.Row) error
	Lock(ctx context.Context, ttl time.Duration) (release func(), ok bool, err error)
}

// Syncer keeps Redis equal to the API's latest snapshot.
type Syncer struct {
	Source      Source
	Store       Store
	Publisher   Publisher
	IndexFields []string
	Log         *slog.Logger
}

// Once runs a single cycle:
//   - nothing changed (local copy and Redis already at the API version): nothing to do;
//   - Redis lost its data but the local copy is current: rebuild Redis from the local copy;
//   - otherwise: download the snapshot, keep it locally and publish it.
//
// On any error Redis keeps serving what it already has.
func (s *Syncer) Once(ctx context.Context) error {
	release, ok, err := s.Publisher.Lock(ctx, lockTTL)
	if err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	if !ok {
		s.Log.Info("another instance is synchronizing; skipping this cycle")
		return nil
	}
	defer release()

	info, err := s.Source.Version(ctx)
	if err != nil {
		return err
	}
	stored, have, err := s.Store.Version(ctx)
	if err != nil {
		return fmt.Errorf("local store: %w", err)
	}
	current, err := s.Publisher.Current(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	switch {
	case have && stored == info.Version && current == info.Version:
		s.Log.Debug("no changes", "version", info.Version)
		return nil
	case have && stored == info.Version:
		s.Log.Info("redis is not at the current version; rebuilding it from the local copy", "version", info.Version, "redis", current)
		snap, found, err := s.Store.Load(ctx)
		if err != nil {
			return fmt.Errorf("local store: %w", err)
		}
		if !found {
			return fmt.Errorf("local store lost its snapshot")
		}
		return s.publish(ctx, snap)
	}

	s.Log.Info("new version; downloading the snapshot", "version", info.Version, "previous", stored)
	snap, err := s.Source.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.Store.Save(ctx, snap); err != nil {
		// The copy only saves recalculation work: Redis must still get the data.
		s.Log.Warn("could not save the local copy; publishing anyway", "error", err)
	}
	return s.publish(ctx, snap)
}

func (s *Syncer) publish(ctx context.Context, snap model.Snapshot) error {
	rows, err := snap.Rows(s.IndexFields)
	if err != nil {
		return fmt.Errorf("snapshot %s: %w", snap.Version, err)
	}
	start := time.Now()
	if err := s.Publisher.Publish(ctx, snap, rows); err != nil {
		return fmt.Errorf("publishing %s: %w", snap.Version, err)
	}
	s.Log.Info("published", "version", snap.Version, "rows", len(rows), "took", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// Run executes a cycle now and then every interval until ctx is cancelled. A failed cycle is
// logged and retried at the next tick.
func (s *Syncer) Run(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := s.Once(ctx); err != nil && ctx.Err() == nil {
			s.Log.Error("sync cycle failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
