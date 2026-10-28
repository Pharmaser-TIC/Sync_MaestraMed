package publish

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// searchSetup connects to a real Redis with the search and JSON modules, which miniredis does
// not emulate. Set NT_TEST_REDIS_ADDR (e.g. 127.0.0.1:16379) to run these tests; they only
// touch keys under a unique prefix.
func searchSetup(t *testing.T) (*Publisher, *redis.Client) {
	t.Helper()
	addr := os.Getenv("NT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NT_TEST_REDIS_ADDR is not set; skipping the RediSearch tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	prefix := fmt.Sprintf("nttest%d", time.Now().UnixNano())
	p := New(rdb, prefix, 2).WithSearch([]string{"nombre"})
	if err := p.CheckSearch(context.Background()); err != nil {
		t.Skipf("the Redis at %s has no search module: %v", addr, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, v := range []string{"v1", "v2", "v3"} {
			p.deleteVersion(ctx, v)
		}
		rdb.Del(ctx, p.key("actual"), p.key("versiones"), p.key("lock"))
		p.rdb.Do(ctx, "FT.ALIASDEL", p.AliasName())
	})
	return p, rdb
}

// find returns the sorted row ids matching a query through the alias readers use.
func find(t *testing.T, rdb *redis.Client, p *Publisher, query string) []string {
	t.Helper()
	res, err := rdb.FTSearchWithArgs(context.Background(), p.AliasName(), query, &redis.FTSearchOptions{NoContent: true}).Result()
	if err != nil {
		t.Fatalf("FT.SEARCH %q: %v", query, err)
	}
	var ids []string
	for _, d := range res.Docs {
		ids = append(ids, strings.TrimPrefix(d.ID, p.key("v1", "doc")+":"))
	}
	slices.Sort(ids)
	return ids
}

func TestSearchFindsRowsByPartOfTheName(t *testing.T) {
	p, rdb := searchSetup(t)
	ctx := context.Background()
	snap, rows := build(t, "v1",
		`{"id":1,"nombre":"ACETAMINOFEN 500 MG TABLETA","estado":"OK"}`,
		`{"id":2,"nombre":"Acetaminofén jarabe 120 mg/5 ml","estado":"OK"}`,
		`{"id":3,"nombre":"IBUPROFENO 400 MG","estado":"OK"}`,
		`{"id":4,"nombre":"ÁCIDO FÓLICO 5 MG","estado":"OK"}`)
	if err := p.Publish(ctx, snap, rows); err != nil {
		t.Fatal(err)
	}

	// Terms go through Fold, so accents in the data or in the query do not matter.
	if got := find(t, rdb, p, "@nombre:"+Fold("acido")+"*"); !slices.Equal(got, []string{"4"}) {
		t.Errorf("search without accent = %v, want [4]", got)
	}
	if got := find(t, rdb, p, "@nombre:"+Fold("Fólico")); !slices.Equal(got, []string{"4"}) {
		t.Errorf("search with accent = %v, want [4]", got)
	}
	if got := find(t, rdb, p, "@nombre:"+Fold("ACETAMINOFÉN")+"*"); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("accented data and plain data must both match = %v, want [1 2]", got)
	}
	// The stored row is the original one, with the folded text next to it.
	raw, err := rdb.JSONGet(ctx, p.key("v1", "doc", "4"), "$").Result()
	if err != nil || !strings.Contains(raw, `"nombre":"ÁCIDO FÓLICO 5 MG"`) || !strings.Contains(raw, `"_busqueda":{"nombre":"acido folico 5 mg"}`) {
		t.Errorf("document = %s (err %v)", raw, err)
	}

	if got := find(t, rdb, p, "@nombre:acetamin*"); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("prefix search = %v, want [1 2]", got)
	}
	if got := find(t, rdb, p, "@nombre:*profeno*"); !slices.Equal(got, []string{"3"}) {
		t.Errorf("contains search = %v, want [3]", got)
	}
	if got := find(t, rdb, p, "@nombre:jarabe 120"); !slices.Equal(got, []string{"2"}) {
		t.Errorf("several words = %v, want [2]", got)
	}
	// The exact lookup sets keep working next to the search documents.
	if m, _ := rdb.SMembers(ctx, p.key("v1", "idx", "estado", "OK")).Result(); len(m) != 4 {
		t.Errorf("estado index = %v", m)
	}
}

func TestSearchAliasSwitchesAndOldIndexesArePruned(t *testing.T) {
	p, rdb := searchSetup(t)
	ctx := context.Background()
	for i, v := range []string{"v1", "v2", "v3"} {
		s, r := build(t, v, fmt.Sprintf(`{"id":%d,"nombre":"MEDICAMENTO %s"}`, i+1, strings.ToUpper(v)))
		if err := p.Publish(ctx, s, r); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct publication scores
		if got := find(t, rdb, p, "@nombre:"+v); len(got) != 1 {
			t.Fatalf("after publishing %s the alias returned %v", v, got)
		}
	}
	indexes, err := rdb.Do(ctx, "FT._LIST").StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(indexes, p.indexName("v1")) {
		t.Error("the index of the pruned version v1 must be dropped")
	}
	if !slices.Contains(indexes, p.indexName("v2")) || !slices.Contains(indexes, p.indexName("v3")) {
		t.Errorf("the indexes of the two newest versions must be kept, have %v", indexes)
	}
	if n, _ := rdb.Exists(ctx, p.key("v1", "doc", "1")).Result(); n != 0 {
		t.Error("the documents of the pruned version must be removed")
	}
}

func TestRepublishingTheSameVersionWithSearchLeavesOneIndex(t *testing.T) {
	p, rdb := searchSetup(t)
	ctx := context.Background()
	s, r := build(t, "v1", `{"id":1,"nombre":"A"}`, `{"id":2,"nombre":"B"}`)
	for i := 0; i < 2; i++ {
		if err := p.Publish(ctx, s, r); err != nil {
			t.Fatalf("publish %d: %v", i+1, err)
		}
	}
	rep, err := p.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.SearchDocs != 2 {
		t.Errorf("OK=%v docs=%d problems=%v", rep.OK(), rep.SearchDocs, rep.Problems)
	}
	_ = rdb
}

func TestInspectReportsTheSearchIndexAndItsProblems(t *testing.T) {
	p, rdb := searchSetup(t)
	ctx := context.Background()
	s, r := build(t, "v1", `{"id":1,"nombre":"A"}`)
	if err := p.Publish(ctx, s, r); err != nil {
		t.Fatal(err)
	}
	rep, err := p.Inspect(ctx)
	if err != nil || !rep.OK() || !rep.SearchOn || rep.SearchDocs != 1 {
		t.Fatalf("healthy: err=%v OK=%v on=%v docs=%d problems=%v", err, rep.OK(), rep.SearchOn, rep.SearchDocs, rep.Problems)
	}
	var out bytes.Buffer
	rep.Write(&out)
	if !strings.Contains(out.String(), "search documents") {
		t.Errorf("the report must show the search documents:\n%s", out.String())
	}

	if err := rdb.Do(ctx, "FT.DROPINDEX", p.indexName("v1")).Err(); err != nil {
		t.Fatal(err)
	}
	rep, err = p.Inspect(ctx)
	if err != nil || rep.OK() {
		t.Fatalf("a missing index must be reported as a problem: err=%v problems=%v", err, rep.Problems)
	}
}
