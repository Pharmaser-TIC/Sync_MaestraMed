package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"nt-sync/internal/model"
)

type fakeSource struct {
	version       string
	snapshot      model.Snapshot
	versionErr    error
	snapshotCalls int
}

func (f *fakeSource) Version(context.Context) (model.VersionInfo, error) {
	return model.VersionInfo{Version: f.version}, f.versionErr
}
func (f *fakeSource) Snapshot(context.Context) (model.Snapshot, error) {
	f.snapshotCalls++
	return f.snapshot, nil
}

type fakeStore struct {
	snap    model.Snapshot
	have    bool
	saves   int
	saveErr error
}

func (f *fakeStore) Version(context.Context) (string, bool, error) {
	return f.snap.Version, f.have, nil
}
func (f *fakeStore) Load(context.Context) (model.Snapshot, bool, error) {
	return f.snap, f.have, nil
}
func (f *fakeStore) Save(_ context.Context, s model.Snapshot) error {
	f.saves++
	if f.saveErr != nil {
		return f.saveErr
	}
	f.snap, f.have = s, true
	return nil
}

type fakePublisher struct {
	current   string
	published []string
	rowCounts []int
	locked    bool
	released  int
}

func (f *fakePublisher) Current(context.Context) (string, error) { return f.current, nil }
func (f *fakePublisher) Publish(_ context.Context, s model.Snapshot, rows []model.Row) error {
	f.published = append(f.published, s.Version)
	f.rowCounts = append(f.rowCounts, len(rows))
	f.current = s.Version
	return nil
}
func (f *fakePublisher) Lock(context.Context, time.Duration) (func(), bool, error) {
	if f.locked {
		return nil, false, nil
	}
	return func() { f.released++ }, true, nil
}

func snap(version string, n int) model.Snapshot {
	s := model.Snapshot{Version: version}
	for i := 1; i <= n; i++ {
		s.Filas = append(s.Filas, json.RawMessage(`{"id":`+string(rune('0'+i))+`}`))
	}
	return s
}

func newSyncer(src *fakeSource, st *fakeStore, pub *fakePublisher) *Syncer {
	return &Syncer{Source: src, Store: st, Publisher: pub, IndexFields: []string{"id"}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestFirstRunDownloadsStoresAndPublishes(t *testing.T) {
	src := &fakeSource{version: "v1", snapshot: snap("v1", 2)}
	st, pub := &fakeStore{}, &fakePublisher{}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.snapshotCalls != 1 || st.saves != 1 || len(pub.published) != 1 || pub.published[0] != "v1" || pub.rowCounts[0] != 2 {
		t.Fatalf("snapshotCalls=%d saves=%d published=%v rows=%v", src.snapshotCalls, st.saves, pub.published, pub.rowCounts)
	}
	if pub.released != 1 {
		t.Errorf("lock released %d times, want 1", pub.released)
	}
}

func TestNothingChangedDoesNothing(t *testing.T) {
	src := &fakeSource{version: "v1"}
	st := &fakeStore{snap: snap("v1", 1), have: true}
	pub := &fakePublisher{current: "v1"}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.snapshotCalls != 0 || st.saves != 0 || len(pub.published) != 0 {
		t.Fatalf("expected no work, got snapshotCalls=%d saves=%d published=%v", src.snapshotCalls, st.saves, pub.published)
	}
}

func TestNewVersionReplacesStoredAndPublished(t *testing.T) {
	src := &fakeSource{version: "v2", snapshot: snap("v2", 3)}
	st := &fakeStore{snap: snap("v1", 1), have: true}
	pub := &fakePublisher{current: "v1"}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.snapshotCalls != 1 || st.snap.Version != "v2" || pub.current != "v2" {
		t.Fatalf("snapshotCalls=%d stored=%s current=%s", src.snapshotCalls, st.snap.Version, pub.current)
	}
}

func TestRedisLostItsDataIsRebuiltFromTheStoreWithoutCallingTheAPI(t *testing.T) {
	src := &fakeSource{version: "v1"}
	st := &fakeStore{snap: snap("v1", 2), have: true}
	pub := &fakePublisher{current: ""} // Redis was flushed
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.snapshotCalls != 0 {
		t.Fatalf("the API must not be asked to calculate again, calls=%d", src.snapshotCalls)
	}
	if len(pub.published) != 1 || pub.published[0] != "v1" || pub.rowCounts[0] != 2 {
		t.Fatalf("published=%v rows=%v", pub.published, pub.rowCounts)
	}
}

func TestSnapshotNewerThanVersionIsPublishedWithItsOwnVersion(t *testing.T) {
	// Data changed between nt_version and nt_snapshot: the snapshot carries the newer version.
	src := &fakeSource{version: "v1", snapshot: snap("v2", 1)}
	st, pub := &fakeStore{}, &fakePublisher{}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pub.current != "v2" || st.snap.Version != "v2" {
		t.Fatalf("current=%s stored=%s", pub.current, st.snap.Version)
	}
}

func TestAnotherInstanceHoldingTheLockSkipsTheCycle(t *testing.T) {
	src := &fakeSource{version: "v1", snapshot: snap("v1", 1)}
	st, pub := &fakeStore{}, &fakePublisher{locked: true}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.snapshotCalls != 0 || len(pub.published) != 0 {
		t.Fatal("must not work while another instance holds the lock")
	}
}

func TestStoreFailureStillPublishes(t *testing.T) {
	src := &fakeSource{version: "v1", snapshot: snap("v1", 1)}
	st, pub := &fakeStore{saveErr: errors.New("disk full")}, &fakePublisher{}
	if err := newSyncer(src, st, pub).Once(context.Background()); err != nil {
		t.Fatalf("a store failure must not stop publishing: %v", err)
	}
	if pub.current != "v1" {
		t.Fatalf("current=%s", pub.current)
	}
}

func TestAPIErrorKeepsWhatIsPublished(t *testing.T) {
	src := &fakeSource{versionErr: errors.New("HTTP 401")}
	st := &fakeStore{snap: snap("v1", 1), have: true}
	pub := &fakePublisher{current: "v1"}
	if err := newSyncer(src, st, pub).Once(context.Background()); err == nil {
		t.Fatal("expected the API error to be returned")
	}
	if len(pub.published) != 0 || pub.current != "v1" {
		t.Fatal("a failing API must leave Redis untouched")
	}
	if pub.released != 1 {
		t.Errorf("lock must be released after an error, released=%d", pub.released)
	}
}
