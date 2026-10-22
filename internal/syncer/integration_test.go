package syncer_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nt-sync/internal/api"
	"nt-sync/internal/publish"
	"nt-sync/internal/store"
	"nt-sync/internal/syncer"
)

// TestEndToEnd wires the real client, store and publisher against a fake API and an in-memory Redis.
func TestEndToEnd(t *testing.T) {
	var version atomic.Value
	version.Store("v1")
	var snapshots atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-API-Key") != "k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Query().Get("accion") {
		case "nt_version":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": version.Load(), "items": 2, "cargas": map[string]int{}})
		case "nt_snapshot":
			snapshots.Add(1)
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			_ = json.NewEncoder(gz).Encode(map[string]any{
				"version": version.Load(), "generado_en": "2026-10-22 10:00:00", "total": 2, "con_precios": false,
				"bases_faltantes": []string{},
				"filas": []map[string]any{
					{"id": 1, "cum_facturar": "123-1", "estado": "VALIDADO", "precio_venta": nil},
					{"id": 2, "cum_facturar": "55-2", "estado": "ERROR", "precio_venta": nil},
				},
			})
			_ = gz.Close()
		}
	}))
	defer srv.Close()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	st, err := store.Open(filepath.Join(t.TempDir(), "nt.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	s := &syncer.Syncer{
		Source:      api.New(srv.URL+"/api.php", "k"),
		Store:       st,
		Publisher:   publish.New(rdb, "nt", 2),
		IndexFields: []string{"cum_facturar", "estado"},
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx := context.Background()

	// 1. First cycle publishes v1.
	if err := s.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.Get(ctx, "nt:actual").Result(); got != "v1" {
		t.Fatalf("pointer = %q", got)
	}
	if m, _ := rdb.SMembers(ctx, "nt:v1:idx:cum_facturar:123-1").Result(); len(m) != 1 || m[0] != "1" {
		t.Fatalf("index = %v", m)
	}
	row, _ := rdb.HGet(ctx, "nt:v1:filas", "2").Result()
	if row == "" {
		t.Fatal("row 2 missing")
	}

	// 2. Same version: the API is not asked to calculate again.
	if err := s.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshots.Load() != 1 {
		t.Fatalf("snapshot requested %d times, want 1", snapshots.Load())
	}

	// 3. Redis is flushed: rebuilt from SQLite, still without a new snapshot.
	mr.FlushAll()
	if err := s.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.Get(ctx, "nt:actual").Result(); got != "v1" || snapshots.Load() != 1 {
		t.Fatalf("after flush pointer=%q snapshots=%d", got, snapshots.Load())
	}

	// 4. The API publishes v2: downloaded once and switched.
	version.Store("v2")
	if err := s.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.Get(ctx, "nt:actual").Result(); got != "v2" || snapshots.Load() != 2 {
		t.Fatalf("after v2 pointer=%q snapshots=%d", got, snapshots.Load())
	}
}
