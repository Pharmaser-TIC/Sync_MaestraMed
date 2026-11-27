package publish

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func publishVersions(t *testing.T, p *Publisher, versions ...string) {
	t.Helper()
	for _, v := range versions {
		s, r := build(t, v, `{"id":1,"estado":"OK"}`)
		if err := p.Publish(context.Background(), s, r); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct publication scores
	}
}

func TestVersionsAreListedOldestFirstAndOnlyWhileTheyAreKept(t *testing.T) {
	p, _ := setup(t, 3)
	ctx := context.Background()
	if got, err := p.Versions(ctx); err != nil || len(got) != 0 {
		t.Fatalf("nothing published: %v %v", got, err)
	}
	// the order is the publication order, not the alphabetical one
	publishVersions(t, p, "vb", "va", "vc")
	if got, _ := p.Versions(ctx); !slices.Equal(got, []string{"vb", "va", "vc"}) {
		t.Errorf("Versions = %v, want publication order [vb va vc]", got)
	}
	publishVersions(t, p, "vd") // keep=3 drops the oldest
	if got, _ := p.Versions(ctx); !slices.Equal(got, []string{"va", "vc", "vd"}) {
		t.Errorf("after pruning: %v", got)
	}
}

func TestMetaOfReadsAnOlderVersionAndRefusesOnesThatAreNotPublished(t *testing.T) {
	p, mr := setup(t, 2)
	ctx := context.Background()
	publishVersions(t, p, "v1", "v2")

	m1, err := p.MetaOf(ctx, "v1")
	if err != nil || m1.Version != "v1" || m1.Total != 1 || m1.PublicadoEn == "" {
		t.Fatalf("%+v %v", m1, err)
	}
	cur, err := p.Meta(ctx)
	if err != nil || cur.Version != "v2" {
		t.Fatalf("Meta (current) must be unchanged: %+v %v", cur, err)
	}
	if m2, _ := p.MetaOf(ctx, "v2"); m2 != cur {
		t.Errorf("MetaOf the current version = %+v, Meta = %+v", m2, cur)
	}

	// never listed (half-written) and unknown versions are not readable even if a meta hash exists
	mr.HSet("nt:v9:meta", "version", "v9", "total", "5")
	for _, v := range []string{"v9", "nope"} {
		if _, err := p.MetaOf(ctx, v); !errors.Is(err, ErrVersionGone) {
			t.Errorf("MetaOf(%q) = %v, want ErrVersionGone", v, err)
		}
	}
}

func TestSearchInAVersionThatIsNotPublishedIsRefused(t *testing.T) {
	p, _ := setup(t, 2)
	publishVersions(t, p, "v1")
	if _, _, err := p.SearchIDsIn(context.Background(), "v9", "@nombre:a*", 0, 10); !errors.Is(err, ErrVersionGone) {
		t.Errorf("err = %v, want ErrVersionGone", err)
	}
}
