package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"nt-sync/internal/model"
)

func snap(version string, ids ...string) model.Snapshot {
	s := model.Snapshot{Version: version, GeneradoEn: "2026-10-22 10:00:00", Total: len(ids), ConPrecios: true,
		BasesFaltantes: json.RawMessage(`["INVIMA"]`)}
	for _, id := range ids {
		s.Filas = append(s.Filas, json.RawMessage(`{"id":`+id+`}`))
	}
	return s
}

func TestSaveLoadRoundTripAndReplace(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "nt.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, ok, err := st.Version(ctx); err != nil || ok {
		t.Fatalf("empty store: ok=%v err=%v", ok, err)
	}
	if err := st.Save(ctx, snap("v1", "1", "2", "3")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if got.Version != "v1" || len(got.Filas) != 3 || string(got.Filas[2]) != `{"id":3}` || !got.ConPrecios || string(got.BasesFaltantes) != `["INVIMA"]` {
		t.Fatalf("Load = %+v", got)
	}

	if err := st.Save(ctx, snap("v2", "9")); err != nil {
		t.Fatal(err)
	}
	got, _, _ = st.Load(ctx)
	if got.Version != "v2" || len(got.Filas) != 1 {
		t.Fatalf("a new Save must replace the previous snapshot, got %+v", got)
	}
	if v, ok, _ := st.Version(ctx); !ok || v != "v2" {
		t.Fatalf("Version = %q, %v", v, ok)
	}
}

func TestSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nt.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Save(ctx, snap("v1", "1")); err != nil {
		t.Fatal(err)
	}
	st.Close()

	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if v, ok, _ := st.Version(ctx); !ok || v != "v1" {
		t.Fatalf("after reopen Version = %q, %v", v, ok)
	}
}
