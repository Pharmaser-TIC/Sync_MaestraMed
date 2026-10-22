package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func server(t *testing.T, key string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-API-Key") != key {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Llave de API inválida."}`))
			return
		}
		switch r.URL.Query().Get("accion") {
		case "nt_version":
			_, _ = w.Write([]byte(`{"version":"abc","items":2,"cargas":{"INVIMA":5}}`))
		case "nt_snapshot":
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			_ = json.NewEncoder(gz).Encode(map[string]any{
				"version": "abc", "generado_en": "2026-10-22 10:00:00", "total": 2, "con_precios": false,
				"bases_faltantes": []string{}, "filas": []map[string]any{{"id": 1}, {"id": 2}},
			})
			_ = gz.Close()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestVersionAndSnapshot(t *testing.T) {
	srv := server(t, "secret")
	c := New(srv.URL+"/api.php", "secret")

	v, err := c.Version(context.Background())
	if err != nil || v.Version != "abc" || v.Items != 2 || v.Cargas["INVIMA"] != 5 {
		t.Fatalf("Version = %+v, %v", v, err)
	}
	s, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Version != "abc" || s.Total != 2 || len(s.Filas) != 2 || s.ConPrecios {
		t.Fatalf("Snapshot = %+v", s)
	}
}

func TestWrongKeyReportsServerMessageWithoutLeakingTheKey(t *testing.T) {
	srv := server(t, "secret")
	_, err := New(srv.URL+"/api.php", "wrong-key-value").Version(context.Background())
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "Llave de API inválida.") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), "wrong-key-value") {
		t.Fatal("the key must never appear in errors")
	}
}

func TestNonJSONAnswerIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<b>Warning</b>: something"))
	}))
	defer srv.Close()
	if _, err := New(srv.URL, "k").Version(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
}
