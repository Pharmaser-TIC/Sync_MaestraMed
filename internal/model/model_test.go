package model

import (
	"encoding/json"
	"testing"
)

func raws(t *testing.T, items ...string) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, len(items))
	for i, s := range items {
		out[i] = json.RawMessage(s)
	}
	return out
}

func TestRowsKeyAndIndex(t *testing.T) {
	s := Snapshot{Filas: raws(t,
		`{"id": 7, "cum_facturar": " 123-1 ", "estado": "validado", "cod_auf": null}`,
		`{"id": "9", "cum_facturar": "55", "estado": "ERROR"}`,
	)}
	rows, err := s.Rows([]string{"cum_facturar", "estado", "cod_auf"})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Key != "7" || rows[1].Key != "9" {
		t.Fatalf("keys = %q, %q", rows[0].Key, rows[1].Key)
	}
	if got := rows[0].Index["cum_facturar"]; got != "123-1" {
		t.Errorf("cum_facturar = %q, want trimmed value", got)
	}
	if got := rows[0].Index["estado"]; got != "VALIDADO" {
		t.Errorf("estado = %q, want upper-cased", got)
	}
	if _, ok := rows[0].Index["cod_auf"]; ok {
		t.Error("null field must not be indexed")
	}
}

func TestRowsDuplicateAndMissingID(t *testing.T) {
	s := Snapshot{Filas: raws(t, `{"id": 4}`, `{"id": 4}`, `{"id": 4}`, `{"x": 1}`)}
	rows, err := s.Rows(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"4", "4#2", "4#3", "n4"}
	for i, w := range want {
		if rows[i].Key != w {
			t.Errorf("row %d key = %q, want %q", i, rows[i].Key, w)
		}
	}
}

func TestRowsKeepsRawJSON(t *testing.T) {
	raw := `{"id":1,"motivos":[{"codigo":"NT01"}]}`
	rows, err := Snapshot{Filas: raws(t, raw)}.Rows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0].JSON) != raw {
		t.Errorf("row JSON changed: %s", rows[0].JSON)
	}
}

func TestRowsRejectsNonObject(t *testing.T) {
	if _, err := (Snapshot{Filas: raws(t, `[1,2]`)}).Rows(nil); err == nil {
		t.Fatal("expected an error for a row that is not an object")
	}
}
