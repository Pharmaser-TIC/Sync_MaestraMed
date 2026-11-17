package publish

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// events returns the entries of the change stream, oldest first.
func events(t *testing.T, p *Publisher) []map[string]string {
	t.Helper()
	msgs, err := p.rdb.XRange(context.Background(), p.ChangesStream(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]map[string]string, 0, len(msgs))
	for _, m := range msgs {
		e := map[string]string{}
		for k, v := range m.Values {
			e[k] = v.(string)
		}
		out = append(out, e)
	}
	return out
}

func publishAll(t *testing.T, p *Publisher, version string, filas ...string) {
	t.Helper()
	s, r := build(t, version, filas...)
	if err := p.Publish(context.Background(), s, r); err != nil {
		t.Fatal(err)
	}
}

func TestFirstPublishHasNoBaselineAndEmitsNoEvents(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"estado":"OK"}`, `{"id":2,"estado":"OK"}`)
	if ev := events(t, p); len(ev) != 0 {
		t.Fatalf("the first version has nothing to compare with, got %v", ev)
	}
}

func TestChangeStreamReportsCreatedModifiedAndDeletedRows(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1",
		`{"id":1,"estado":"OK","precio":10}`,
		`{"id":2,"estado":"OK","precio":20}`,
		`{"id":3,"estado":"OK","precio":30}`)
	publishAll(t, p, "v2",
		`{"id":1,"estado":"OK","precio":10}`,    // unchanged
		`{"id":2,"estado":"ERROR","precio":25}`, // modified
		`{"id":4,"estado":"OK","precio":40}`)    // created; 3 was deleted

	byID := map[string]map[string]string{}
	for _, e := range events(t, p) {
		if _, dup := byID[e["id"]]; dup {
			t.Fatalf("two events for row %s", e["id"])
		}
		byID[e["id"]] = e
	}
	if len(byID) != 3 {
		t.Fatalf("want events for rows 2, 3 and 4 only, got %v", byID)
	}

	mod := byID["2"]
	if mod["tipo"] != "modificado" || mod["version"] != "v2" || mod["anterior"] != "v1" {
		t.Errorf("row 2 = %v", mod)
	}
	if mod["campos"] != "estado,precio" {
		t.Errorf("row 2 campos = %q", mod["campos"])
	}
	var antes map[string]any
	if err := json.Unmarshal([]byte(mod["antes"]), &antes); err != nil || !reflect.DeepEqual(antes, map[string]any{"estado": "OK", "precio": float64(20)}) {
		t.Errorf("row 2 antes = %q (%v)", mod["antes"], err)
	}
	if mod["fila"] != `{"id":2,"estado":"ERROR","precio":25}` {
		t.Errorf("row 2 fila = %q", mod["fila"])
	}

	if c := byID["4"]; c["tipo"] != "creado" || c["fila"] != `{"id":4,"estado":"OK","precio":40}` || c["version"] != "v2" {
		t.Errorf("row 4 = %v", c)
	}
	if d := byID["3"]; d["tipo"] != "eliminado" || d["fila"] != `{"id":3,"estado":"OK","precio":30}` || d["version"] != "v2" {
		t.Errorf("row 3 = %v", d)
	}
}

func TestAFieldThatAppearsOrDisappearsIsReportedAsChanged(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x","b":"y"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"x","c":"z"}`)
	ev := events(t, p)
	if len(ev) != 1 || ev[0]["campos"] != "b,c" {
		t.Fatalf("events = %v", ev)
	}
	var antes map[string]any
	_ = json.Unmarshal([]byte(ev[0]["antes"]), &antes)
	if !reflect.DeepEqual(antes, map[string]any{"b": "y", "c": nil}) {
		t.Errorf("antes = %v", antes)
	}
}

func TestSameDataWithAnotherKeyOrderIsNotAChange(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x","b":"y"}`)
	publishAll(t, p, "v2", `{"b":"y","id":1,"a":"x"}`)
	if ev := events(t, p); len(ev) != 0 {
		t.Fatalf("a different key order is not a change, got %v", ev)
	}
}

func TestRepublishingTheSameVersionEmitsNoEvents(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	if ev := events(t, p); len(ev) != 0 {
		t.Fatalf("got %v", ev)
	}
}

func TestChangeStreamIsOffUnlessConfigured(t *testing.T) {
	p, mr := setup(t, 2)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"changed"}`)
	if mr.Exists(p.ChangesStream()) {
		t.Fatal("the change stream must not be written unless WithChanges is used")
	}

	p2, mr2 := setup(t, 2)
	p2.WithChanges(0)
	publishAll(t, p2, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p2, "v2", `{"id":1,"a":"changed"}`)
	if mr2.Exists(p2.ChangesStream()) {
		t.Fatal("WithChanges(0) must keep the stream off")
	}
}

func TestEventsAreWrittenBeforeReadersSwitchToTheNewVersion(t *testing.T) {
	// A consumer that sees nt:actual = v2 must already find v2's events: at-least-once.
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"y"}`)
	cur, _ := p.Current(context.Background())
	ev := events(t, p)
	if cur != "v2" || len(ev) != 1 || ev[0]["version"] != "v2" {
		t.Fatalf("current=%q events=%v", cur, ev)
	}
}

func TestPrunedVersionsDoNotRemoveTheChangeStream(t *testing.T) {
	p, mr := setup(t, 1)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"y"}`)
	publishAll(t, p, "v3", `{"id":1,"a":"z"}`)
	if !mr.Exists("nt:v2:filas") && !mr.Exists("nt:v3:filas") {
		t.Fatal("setup: expected recent versions to exist")
	}
	if ev := events(t, p); len(ev) != 2 {
		t.Fatalf("events from v2 and v3 must survive pruning, got %v", ev)
	}
}

func TestLostBaselineEmitsNoFlood(t *testing.T) {
	// nt:actual points to a version whose rows are gone: there is nothing to compare with.
	p, mr := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	mr.Del("nt:v1:filas")
	publishAll(t, p, "v2", `{"id":1,"a":"y"}`, `{"id":2,"a":"z"}`)
	if ev := events(t, p); len(ev) != 0 {
		t.Fatalf("got %v", ev)
	}
}

func TestInspectReportsTheChangeStream(t *testing.T) {
	p, _ := setup(t, 2)
	p.WithChanges(1000)
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"y"}`)
	r, err := p.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !r.ChangesOn || r.ChangesLen != 1 || r.ChangesEnd == "" {
		t.Fatalf("report = %+v", r)
	}
	var out strings.Builder
	r.Write(&out)
	if !strings.Contains(out.String(), "Change stream nt:cambios: 1 entries") {
		t.Errorf("report text:\n%s", out.String())
	}
}

func TestCounterIsSeededFromAStreamWrittenBeforeTheCounterExisted(t *testing.T) {
	// an earlier binary appended entries without keeping nt:cambios:total
	p, mr := setup(t, 2)
	p.WithChanges(1000)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := p.rdb.XAdd(ctx, &redis.XAddArgs{Stream: p.ChangesStream(), Values: []any{"tipo", "creado", "id", "x", "version", "v0", "anterior", "", "fila", "{}"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	publishAll(t, p, "v1", `{"id":1,"a":"x"}`)
	publishAll(t, p, "v2", `{"id":1,"a":"y"}`) // one more entry
	if got, _ := mr.Get(p.changesTotalKey()); got != "3" {
		t.Fatalf("total = %q, want 3 (2 existing + 1 new): otherwise trimming of old entries goes unnoticed", got)
	}
}

func TestDeletingAVersionAlsoRemovesItFromTheVersionList(t *testing.T) {
	p, mr := setup(t, 2)
	publishAll(t, p, "v1", `{"id":1}`)
	if err := p.deleteVersion(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if z, _ := mr.ZMembers("nt:versiones"); len(z) != 0 {
		t.Fatalf("a version that is being rewritten must not stay listed as readable: %v", z)
	}
}
