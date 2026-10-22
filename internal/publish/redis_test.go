package publish

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nt-sync/internal/model"
)

func setup(t *testing.T, keep int) (*Publisher, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb, "nt", keep), mr
}

func build(t *testing.T, version string, filas ...string) (model.Snapshot, []model.Row) {
	t.Helper()
	s := model.Snapshot{Version: version, GeneradoEn: "2026-10-22 10:00:00", ConPrecios: true, BasesFaltantes: json.RawMessage(`["INVIMA"]`)}
	for _, f := range filas {
		s.Filas = append(s.Filas, json.RawMessage(f))
	}
	rows, err := s.Rows([]string{"cum_facturar", "estado"})
	if err != nil {
		t.Fatal(err)
	}
	return s, rows
}

func TestPublishWritesRowsIndexesMetaAndPointer(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	snap, rows := build(t, "v1",
		`{"id":1,"cum_facturar":"123-1","estado":"VALIDADO"}`,
		`{"id":2,"cum_facturar":"123-1","estado":"ERROR"}`)
	if err := p.Publish(ctx, snap, rows); err != nil {
		t.Fatal(err)
	}

	if cur, _ := p.Current(ctx); cur != "v1" {
		t.Fatalf("Current = %q", cur)
	}
	if got := mr.HGet("nt:v1:filas", "2"); got != `{"id":2,"cum_facturar":"123-1","estado":"ERROR"}` {
		t.Errorf("row 2 = %q", got)
	}
	members, _ := mr.SMembers("nt:v1:idx:cum_facturar:123-1")
	if len(members) != 2 {
		t.Errorf("cum_facturar index = %v", members)
	}
	if m, _ := mr.SMembers("nt:v1:idx:estado:ERROR"); len(m) != 1 || m[0] != "2" {
		t.Errorf("estado index = %v", m)
	}
	if mr.HGet("nt:v1:meta", "total") != "2" || mr.HGet("nt:v1:meta", "con_precios") != "1" || mr.HGet("nt:v1:meta", "bases_faltantes") != `["INVIMA"]` {
		t.Errorf("meta = %v", mr.HGet("nt:v1:meta", "total"))
	}
}

func TestPreviousVersionStaysReadableAfterTheSwitch(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	s1, r1 := build(t, "v1", `{"id":1}`)
	if err := p.Publish(ctx, s1, r1); err != nil {
		t.Fatal(err)
	}
	s2, r2 := build(t, "v2", `{"id":1}`, `{"id":2}`)
	if err := p.Publish(ctx, s2, r2); err != nil {
		t.Fatal(err)
	}
	if cur, _ := p.Current(ctx); cur != "v2" {
		t.Fatalf("Current = %q", cur)
	}
	if !mr.Exists("nt:v1:filas") {
		t.Error("the previous version must still be readable right after the switch")
	}
}

func TestPublishPrunesOldVersions(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	for _, v := range []string{"v1", "v2", "v3"} {
		s, r := build(t, v, `{"id":1,"estado":"OK"}`)
		if err := p.Publish(ctx, s, r); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct publication scores
	}
	for _, k := range mr.Keys() {
		if strings.HasPrefix(k, "nt:v1:") {
			t.Errorf("v1 should have been removed, found %s", k)
		}
	}
	if !mr.Exists("nt:v2:filas") || !mr.Exists("nt:v3:filas") {
		t.Error("the two newest versions must be kept")
	}
	if cur, _ := p.Current(ctx); cur != "v3" {
		t.Errorf("Current = %q", cur)
	}
}

func TestRepublishingTheSameVersionLeavesNoDuplicates(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	s, r := build(t, "v1", `{"id":1,"estado":"OK"}`, `{"id":2,"estado":"OK"}`)
	for i := 0; i < 2; i++ {
		if err := p.Publish(ctx, s, r); err != nil {
			t.Fatal(err)
		}
	}
	if m, _ := mr.SMembers("nt:v1:idx:estado:OK"); len(m) != 2 {
		t.Errorf("index = %v", m)
	}
	if z, _ := mr.ZMembers("nt:versiones"); len(z) != 1 {
		t.Errorf("versiones = %v", z)
	}
}

func TestCurrentWithNothingPublished(t *testing.T) {
	p, _ := setup(t, 2)
	if cur, err := p.Current(context.Background()); err != nil || cur != "" {
		t.Fatalf("Current = %q, %v", cur, err)
	}
}

func TestLockIsExclusiveUntilReleased(t *testing.T) {
	p, _ := setup(t, 2)
	ctx := context.Background()
	release, ok, err := p.Lock(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first Lock: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := p.Lock(ctx, time.Minute); ok {
		t.Fatal("a second Lock must fail while the first is held")
	}
	release()
	if _, ok, _ := p.Lock(ctx, time.Minute); !ok {
		t.Fatal("Lock must be available again after release")
	}
}
