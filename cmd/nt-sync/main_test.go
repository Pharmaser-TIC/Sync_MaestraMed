package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nt-sync/internal/config"
	"nt-sync/internal/model"
	"nt-sync/internal/publish"
)

func sha(key string) string {
	s := sha256.Sum256([]byte(key))
	return hex.EncodeToString(s[:])
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestServeHTTPWiresTheConfigConsumersAndRedisTogether(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	pub := publish.New(rdb, "nt", 2)

	snap := model.Snapshot{Version: "v1", GeneradoEn: "2026-11-17 10:00:00", BasesFaltantes: json.RawMessage(`[]`),
		Filas: []json.RawMessage{json.RawMessage(`{"id":1,"cum_facturar":"A-1","nombre":"ACIDO","precio_venta":10}`)}}
	rows, err := snap.Rows([]string{"cum_facturar"})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(context.Background(), snap, rows); err != nil {
		t.Fatal(err)
	}

	file := filepath.Join(t.TempDir(), "consumers.json")
	content := `{"consumers":[
		{"name":"facturacion","key_sha256":"` + sha("full") + `","precios":true},
		{"name":"inventario","key_sha256":"` + sha("restricted") + `","precios":false}]}`
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		IndexFields: []string{"cum_facturar"},
		Redis:       config.Redis{Addr: mr.Addr(), Prefix: "nt"},
		HTTP:        config.HTTP{Addr: freeAddr(t), ConsumersFile: file, PriceFields: []string{"precio_venta"}, Fields: []string{"id", "nombre", "precio_venta"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, pub) }()

	get := func(key string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodGet, "http://"+cfg.HTTP.Addr+"/v1/productos/1", nil)
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, nil
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	var code int
	var full map[string]any
	for i := 0; i < 100; i++ { // wait for the listener
		if code, full = get("full"); code != 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 200 || full["precio_venta"] != float64(10) {
		t.Fatalf("consumer with prices: %d %v", code, full)
	}
	if _, has := full["cum_facturar"]; has {
		t.Errorf("NT_API_FIELDS must reach the API: a column that is not listed was returned: %v", full)
	}
	if code, restricted := get("restricted"); code != 200 || restricted["nombre"] != "ACIDO" || restricted["precio_venta"] != nil {
		t.Fatalf("consumer without prices: %d %v", code, restricted)
	}
	if code, _ := get("nope"); code != http.StatusUnauthorized {
		t.Errorf("unknown key: %d", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("graceful shutdown returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not stop after the context was cancelled")
	}
}

func TestServeHTTPDoesNotStartWithoutAUsableConsumersFile(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	cfg := config.Config{HTTP: config.HTTP{Addr: freeAddr(t), ConsumersFile: filepath.Join(t.TempDir(), "missing.json"), PriceFields: []string{"precio_venta"}}}
	if err := serveHTTP(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, publish.New(rdb, "nt", 2)); err == nil {
		t.Fatal("a server without consumers must refuse to start")
	}
}
