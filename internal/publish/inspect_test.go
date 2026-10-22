package publish

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestInspectReportsWhatWasPublished(t *testing.T) {
	p, _ := setup(t, 2)
	ctx := context.Background()
	snap, rows := build(t, "v1",
		`{"id":1,"cum_facturar":"123-1","estado":"VALIDADO"}`,
		`{"id":2,"cum_facturar":"456-1","estado":"ERROR"}`)
	if err := p.Publish(ctx, snap, rows); err != nil {
		t.Fatal(err)
	}

	r, err := p.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) != 0 {
		t.Fatalf("unexpected problems: %v", r.Problems)
	}
	if r.Current != "v1" || r.Rows != 2 || r.Meta["total"] != "2" {
		t.Errorf("current=%q rows=%d meta=%v", r.Current, r.Rows, r.Meta)
	}
	if r.Indexes["cum_facturar"] != 2 || r.Indexes["estado"] != 2 {
		t.Errorf("indexes = %v", r.Indexes)
	}
	if r.SampleKey == "" || !strings.HasPrefix(r.SampleJSON, "{") {
		t.Errorf("sample = %q %q", r.SampleKey, r.SampleJSON)
	}
	if len(r.Versions) != 1 || r.Versions[0].Version != "v1" || r.Versions[0].Rows != 2 {
		t.Errorf("versions = %+v", r.Versions)
	}

	var out bytes.Buffer
	r.Write(&out)
	for _, want := range []string{"nt:actual", "v1", "total", "cum_facturar", "OK"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report does not mention %q:\n%s", want, out.String())
		}
	}
}

func TestInspectFlagsAnEmptyRedis(t *testing.T) {
	p, _ := setup(t, 2)
	r, err := p.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) == 0 || !strings.Contains(r.Problems[0], "nt:actual") {
		t.Errorf("problems = %v", r.Problems)
	}
	var out bytes.Buffer
	r.Write(&out)
	if !strings.Contains(out.String(), "PROBLEMS") {
		t.Errorf("report should say PROBLEMS:\n%s", out.String())
	}
}

func TestInspectFlagsMetaTotalThatDoesNotMatchTheStoredRows(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	snap, rows := build(t, "v1", `{"id":1}`, `{"id":2}`)
	if err := p.Publish(ctx, snap, rows); err != nil {
		t.Fatal(err)
	}
	mr.HSet("nt:v1:meta", "total", "99")

	r, err := p.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) != 1 || !strings.Contains(r.Problems[0], "99") {
		t.Errorf("problems = %v", r.Problems)
	}
}

func TestInspectFlagsAPointerToAVersionWithoutRows(t *testing.T) {
	p, mr := setup(t, 2)
	if err := mr.Set("nt:actual", "ghost"); err != nil {
		t.Fatal(err)
	}
	r, err := p.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) == 0 {
		t.Error("a pointer to a version that has no data must be reported")
	}
}

func TestInspectShowsAHeldLock(t *testing.T) {
	p, _ := setup(t, 2)
	ctx := context.Background()
	release, ok, err := p.Lock(ctx, 600e9)
	if err != nil || !ok {
		t.Fatalf("lock: ok=%v err=%v", ok, err)
	}
	defer release()
	r, err := p.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !r.LockHeld || r.LockTTL <= 0 {
		t.Errorf("lock held=%v ttl=%v", r.LockHeld, r.LockTTL)
	}
}
