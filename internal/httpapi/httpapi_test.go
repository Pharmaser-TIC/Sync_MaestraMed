package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nt-sync/internal/config"
	"nt-sync/internal/model"
	"nt-sync/internal/publish"
)

const (
	keyFull   = "key-with-prices"
	keyNoCost = "key-without-prices"
)

func hashOf(key string) string {
	s := sha256.Sum256([]byte(key))
	return hex.EncodeToString(s[:])
}

var testIndexFields = []string{"cum_facturar", "estado", "precio_venta"}

// fakeSearch replaces the RediSearch call, which miniredis does not emulate.
type fakeSearch struct {
	*publish.Publisher
	gotQuery string
	gotVer   string // version of the last pinned search
	ids      []string
	called   bool
}

func (f *fakeSearch) SearchIDsIn(ctx context.Context, ver, query string, offset, limit int) ([]string, int, error) {
	f.called = true
	f.gotQuery = query
	f.gotVer = ver
	return f.ids, len(f.ids), nil
}

func (f *fakeSearch) SearchIDs(ctx context.Context, query string, offset, limit int) (string, []string, int, error) {
	f.called = true
	f.gotQuery = query
	cur, _ := f.Current(ctx)
	return cur, f.ids, len(f.ids), nil
}

type env struct {
	srv *httptest.Server
	rdb *redis.Client
	p   *publish.Publisher
	mr  *miniredis.Miniredis
	fs  *fakeSearch
	idx []string // fields indexed when publishing
}

type envOpts struct {
	keep         int
	searchFields []string
	changesMax   int64
	maxInFlight  int
	wrap         func(Source) Source
	fields       []string // columns the API returns; nil returns every column
	priceFields  []string // nil means precio_venta
	indexFields  []string // nil means testIndexFields
}

func newEnv(t *testing.T, o envOpts) *env {
	t.Helper()
	if o.keep == 0 {
		o.keep = 2
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	if o.changesMax == 0 {
		o.changesMax = 1000
	}
	p := publish.New(rdb, "nt", o.keep).WithChanges(o.changesMax)
	fs := &fakeSearch{Publisher: p}
	priceFields := o.priceFields
	if priceFields == nil {
		priceFields = []string{"precio_venta"}
	}
	if o.indexFields == nil {
		o.indexFields = testIndexFields
	}
	var src Source = fs
	if o.wrap != nil {
		src = o.wrap(fs)
	}
	h, err := New(src, Options{
		MaxInFlight: o.maxInFlight,
		Consumers: []Consumer{
			{Name: "facturacion", KeySHA256: hashOf(keyFull), Precios: true},
			{Name: "inventario", KeySHA256: hashOf(keyNoCost), Precios: false},
		},
		PriceFields:  priceFields,
		Fields:       o.fields,
		IndexFields:  o.indexFields,
		SearchFields: o.searchFields,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &env{srv: srv, rdb: rdb, p: p, mr: mr, fs: fs, idx: o.indexFields}
}

func (e *env) publish(t *testing.T, version string, filas ...string) {
	t.Helper()
	s := model.Snapshot{Version: version, GeneradoEn: "2026-11-17 10:00:00", ConPrecios: true, BasesFaltantes: json.RawMessage(`[]`)}
	for _, f := range filas {
		s.Filas = append(s.Filas, json.RawMessage(f))
	}
	rows, err := s.Rows(e.idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.p.Publish(context.Background(), s, rows); err != nil {
		t.Fatal(err)
	}
}

func (e *env) getRaw(t *testing.T, path, key string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+path, nil)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func (e *env) get(t *testing.T, path, key string) (int, map[string]any) {
	t.Helper()
	code, body := e.getRaw(t, path, key)
	var out map[string]any
	_ = json.Unmarshal([]byte(body), &out)
	return code, out
}

func items(body map[string]any) []map[string]any {
	var out []map[string]any
	for _, x := range body["productos"].([]any) {
		out = append(out, x.(map[string]any))
	}
	return out
}

func ids(body map[string]any) []string {
	var out []string
	for _, r := range items(body) {
		out = append(out, jsonString(r["id"]))
	}
	slices.Sort(out)
	return out
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return strings.Trim(string(b), `"`)
}

var baseRows = []string{
	`{"id":1,"cum_facturar":"123-1","nombre":"ACIDO FOLICO","precio_venta":10,"estado":"OK"}`,
	`{"id":2,"cum_facturar":"123-2","nombre":"LEVOFLOXACINO","precio_venta":20,"estado":"OK"}`,
	`{"id":3,"cum_facturar":"123-3","nombre":"BETAMETIL","precio_venta":30,"estado":"ERROR"}`,
	`{"id":3,"cum_facturar":"123-4","nombre":"BETAMETIL GOTAS","precio_venta":35,"estado":"OK"}`,
	`{"id":5,"cum_facturar":"123-5","nombre":"EPINEFRINA","precio_venta":50,"estado":"OK"}`,
}

func TestEveryRequestNeedsAValidAPIKey(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)

	for _, tc := range []struct{ path, key string }{
		{"/v1/version", ""}, {"/v1/version", "wrong"}, {"/v1/productos", ""},
		{"/v1/productos/1", "wrong"}, {"/v1/cambios?desde=0-0", ""}, {"/nope", ""},
	} {
		if code, _ := e.get(t, tc.path, tc.key); code != http.StatusUnauthorized {
			t.Errorf("%s with key %q: status %d, want 401", tc.path, tc.key, code)
		}
	}

	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/version", nil)
	req.Header.Set("X-API-Key", keyFull)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Cache-Control") != "no-store" ||
		!strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
}

func TestVersionReportsTheCurrentVersionAndTheEndOfTheChangeStream(t *testing.T) {
	e := newEnv(t, envOpts{})
	if code, _ := e.get(t, "/v1/version", keyFull); code != http.StatusServiceUnavailable {
		t.Errorf("nothing published: status %d, want 503", code)
	}
	e.publish(t, "v1", baseRows...)
	code, b := e.get(t, "/v1/version", keyNoCost)
	if code != 200 || b["version"] != "v1" || b["total"] != float64(5) || b["ultimo_cambio"] != "0-0" {
		t.Fatalf("%d %v", code, b)
	}
	e.publish(t, "v2", baseRows[:4]...)
	_, b = e.get(t, "/v1/version", keyNoCost)
	if b["version"] != "v2" || b["ultimo_cambio"] == "0-0" {
		t.Errorf("after a change: %v", b)
	}
}

func TestListPagesThroughEveryProductOnce(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	var all []string
	path := "/v1/productos?limit=2"
	for pages := 0; pages < 10; pages++ {
		code, b := e.get(t, path, keyFull)
		if code != 200 || b["version"] != "v1" {
			t.Fatalf("%d %v", code, b)
		}
		all = append(all, ids(b)...)
		next, _ := b["siguiente"].(string)
		if next == "" {
			break
		}
		path = "/v1/productos?limit=2&cursor=" + url.QueryEscape(next)
	}
	slices.Sort(all)
	if len(all) != 5 {
		t.Fatalf("got %v, want the 5 rows exactly once", all)
	}
}

func TestACursorKeepsReadingTheVersionItStartedOn(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	_, first := e.get(t, "/v1/productos?limit=2", keyFull)
	next := first["siguiente"].(string)

	e.publish(t, "v2", `{"id":99,"cum_facturar":"999","nombre":"NUEVO","precio_venta":1,"estado":"OK"}`)
	code, b := e.get(t, "/v1/productos?limit=100&cursor="+url.QueryEscape(next), keyFull)
	if code != 200 || b["version"] != "v1" {
		t.Fatalf("%d %v", code, b)
	}
	for _, r := range items(b) {
		if r["nombre"] == "NUEVO" {
			t.Error("a paged read must not mix versions")
		}
	}
}

func TestACursorOfARemovedVersionIsGone(t *testing.T) {
	e := newEnv(t, envOpts{keep: 1})
	e.publish(t, "v1", baseRows...)
	_, first := e.get(t, "/v1/productos?limit=2", keyFull)
	next := first["siguiente"].(string)
	e.publish(t, "v2", baseRows[:2]...)
	if code, _ := e.get(t, "/v1/productos?cursor="+url.QueryEscape(next), keyFull); code != http.StatusGone {
		t.Errorf("status %d, want 410", code)
	}
}

func TestBadCursorAndLimitAreRejected(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	for _, q := range []string{"cursor=garbage", "limit=0", "limit=abc", "limit=5000", "limit=-1", "offset=-1", "offset=100001"} {
		if code, _ := e.get(t, "/v1/productos?"+q, keyFull); code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, code)
		}
	}
}

func TestOneProductByRowKey(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	code, b := e.get(t, "/v1/productos/2", keyFull)
	if code != 200 || b["nombre"] != "LEVOFLOXACINO" || b["precio_venta"] != float64(20) {
		t.Fatalf("%d %v", code, b)
	}
	// the second row that shares id 3 is "3#2"; '#' travels as %23
	if code, b = e.get(t, "/v1/productos/3%232", keyFull); code != 200 || b["nombre"] != "BETAMETIL GOTAS" {
		t.Fatalf("%d %v", code, b)
	}
	if code, _ = e.get(t, "/v1/productos/404", keyFull); code != http.StatusNotFound {
		t.Errorf("missing row: status %d, want 404", code)
	}
}

func TestPricesAreHiddenFromConsumersThatMayNotSeeThem(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)

	_, list := e.get(t, "/v1/productos?limit=100", keyNoCost)
	if len(items(list)) != 5 {
		t.Fatalf("the consumer without prices still gets the products: %v", list)
	}
	for _, r := range items(list) {
		if _, has := r["precio_venta"]; has {
			t.Errorf("list leaks the price: %v", r)
		}
		if r["nombre"] == nil || r["cum_facturar"] == nil {
			t.Errorf("other columns must stay: %v", r)
		}
	}
	_, one := e.get(t, "/v1/productos/1", keyNoCost)
	if _, has := one["precio_venta"]; has || one["nombre"] != "ACIDO FOLICO" {
		t.Errorf("single row = %v", one)
	}
	_, byField := e.get(t, "/v1/productos?cum_facturar=123-1", keyNoCost)
	for _, r := range items(byField) {
		if _, has := r["precio_venta"]; has {
			t.Errorf("filter leaks the price: %v", r)
		}
	}

	_, withPrices := e.get(t, "/v1/productos/1", keyFull)
	if withPrices["precio_venta"] != float64(10) {
		t.Errorf("the consumer with prices lost them: %v", withPrices)
	}
}

func TestFilterByAnIndexedFieldNormalizesTheValue(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", `{"id":1,"cum_facturar":"abc-1","estado":"ok"}`, `{"id":2,"cum_facturar":"X","estado":"ERROR"}`)
	code, b := e.get(t, "/v1/productos?cum_facturar="+url.QueryEscape("  ABC-1 "), keyFull)
	if code != 200 || len(items(b)) != 1 || b["total"] != float64(1) {
		t.Fatalf("%d %v", code, b)
	}
	if _, b = e.get(t, "/v1/productos?estado=Error", keyFull); len(items(b)) != 1 {
		t.Errorf("case-insensitive filter: %v", b)
	}
	if _, b = e.get(t, "/v1/productos?estado=NOPE", keyFull); len(items(b)) != 0 {
		t.Errorf("no match must be an empty list: %v", b)
	}
}

func TestFilterRejectsUnknownMixedAndPriceFields(t *testing.T) {
	e := newEnv(t, envOpts{searchFields: []string{"nombre"}})
	e.publish(t, "v1", baseRows...)
	for path, want := range map[string]int{
		"/v1/productos?nombre_raro=1":              http.StatusBadRequest, // not an indexed field
		"/v1/productos?estado=OK&cum_facturar=123": http.StatusBadRequest, // one filter at a time
		"/v1/productos?estado=OK&q=acido":          http.StatusBadRequest, // filter and text search together
		"/v1/productos?precio_venta=10":            http.StatusForbidden,  // asking by price reveals it
	} {
		if code, _ := e.get(t, path, keyNoCost); code != want {
			t.Errorf("%s: status %d, want %d", path, code, want)
		}
	}
	if code, b := e.get(t, "/v1/productos?precio_venta=10", keyFull); code != 200 || len(items(b)) != 1 {
		t.Errorf("a consumer with prices may filter by price: %d %v", code, b)
	}
}

func TestTextSearchBuildsASafeQueryFromTheTermsOnly(t *testing.T) {
	e := newEnv(t, envOpts{searchFields: []string{"nombre"}})
	e.publish(t, "v1", baseRows...)
	e.fs.ids = []string{"2", "1"}

	code, b := e.get(t, "/v1/productos?q="+url.QueryEscape("Ácido fólico"), keyFull)
	if code != 200 || e.fs.gotQuery != "@nombre:(*acido* *folico*)" || len(items(b)) != 2 {
		t.Fatalf("%d query=%q body=%v", code, e.fs.gotQuery, b)
	}

	// RediSearch operators in the input must not reach the query
	e.get(t, "/v1/productos?q="+url.QueryEscape(`x) | @*:(*`+" levo"), keyFull)
	if strings.ContainsAny(e.fs.gotQuery, `|*)(@`) && e.fs.gotQuery != "@nombre:(*levo*)" {
		t.Errorf("unsafe query: %q", e.fs.gotQuery)
	}
	if e.fs.gotQuery != "@nombre:(*levo*)" {
		t.Errorf("query = %q, want only the terms of 2+ letters", e.fs.gotQuery)
	}

	e.fs.called = false
	for _, q := range []string{"a", "%20", "(*)"} {
		if code, _ := e.get(t, "/v1/productos?q="+q, keyFull); code != http.StatusBadRequest {
			t.Errorf("q=%q: status %d, want 400", q, code)
		}
	}
	if e.fs.called {
		t.Error("an invalid text must not reach Redis")
	}
}

func TestTextSearchIsOffWithoutSearchFieldsAndSkipsPriceFieldsForRestrictedConsumers(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	if code, _ := e.get(t, "/v1/productos?q=acido", keyFull); code != http.StatusNotImplemented {
		t.Errorf("search off: status %d, want 501", code)
	}

	e = newEnv(t, envOpts{searchFields: []string{"nombre", "precio_venta"}})
	e.publish(t, "v1", baseRows...)
	e.fs.ids = []string{"1"}
	e.get(t, "/v1/productos?q=acido", keyNoCost)
	if e.fs.gotQuery != "@nombre:(*acido*)" {
		t.Errorf("restricted consumer query = %q", e.fs.gotQuery)
	}
	e.get(t, "/v1/productos?q=acido", keyFull)
	if e.fs.gotQuery != "(@nombre:(*acido*))|(@precio_venta:(*acido*))" {
		t.Errorf("full consumer query = %q", e.fs.gotQuery)
	}

	e = newEnv(t, envOpts{searchFields: []string{"precio_venta"}})
	e.publish(t, "v1", baseRows...)
	if code, _ := e.get(t, "/v1/productos?q=acido", keyNoCost); code != http.StatusForbidden {
		t.Errorf("only price fields searchable: status %d, want 403", code)
	}
}

func events(body map[string]any) []map[string]any {
	var out []map[string]any
	for _, x := range body["eventos"].([]any) {
		out = append(out, x.(map[string]any))
	}
	return out
}

func TestChangesAreReadFromACursorHeldByTheClient(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:2]...)
	_, v := e.get(t, "/v1/version", keyFull)
	cursor := v["ultimo_cambio"].(string)

	e.publish(t, "v2",
		`{"id":1,"cum_facturar":"123-1","nombre":"ACIDO FOLICO","precio_venta":10,"estado":"OK"}`,
		`{"id":2,"cum_facturar":"123-2","nombre":"LEVOFLOXACINO 500","precio_venta":20,"estado":"OK"}`,
		`{"id":7,"cum_facturar":"777","nombre":"NUEVO","precio_venta":7,"estado":"OK"}`)

	code, b := e.get(t, "/v1/cambios?desde="+cursor, keyFull)
	if code != 200 {
		t.Fatalf("%d %v", code, b)
	}
	ev := events(b)
	if len(ev) != 2 || b["hay_mas"] != false {
		t.Fatalf("events = %v", b)
	}
	byID := map[string]map[string]any{}
	for _, x := range ev {
		byID[x["id"].(string)] = x
	}
	mod := byID["2"]
	if mod["tipo"] != "modificado" || mod["version"] != "v2" || mod["anterior"] != "v1" || mod["evento"] == "" {
		t.Errorf("modified = %v", mod)
	}
	if got := mod["campos"].([]any); len(got) != 1 || got[0] != "nombre" {
		t.Errorf("campos = %v", mod["campos"])
	}
	if mod["antes"].(map[string]any)["nombre"] != "LEVOFLOXACINO" || mod["fila"].(map[string]any)["nombre"] != "LEVOFLOXACINO 500" {
		t.Errorf("antes/fila = %v / %v", mod["antes"], mod["fila"])
	}
	if byID["7"]["tipo"] != "creado" {
		t.Errorf("created = %v", byID["7"])
	}

	// the client keeps `siguiente` and finds nothing new until the next publication
	next := b["siguiente"].(string)
	if _, b = e.get(t, "/v1/cambios?desde="+next, keyFull); len(events(b)) != 0 || b["siguiente"] != next {
		t.Errorf("second poll = %v", b)
	}
}

func TestChangesPageWithLimitAndHasMore(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:1]...)
	e.publish(t, "v2", baseRows[1:]...) // 1 deleted, 4 created

	cursor := "0-0"
	total := 0
	for i := 0; i < 10; i++ {
		_, b := e.get(t, "/v1/cambios?limit=2&desde="+cursor, keyFull)
		total += len(events(b))
		cursor = b["siguiente"].(string)
		if b["hay_mas"] == false {
			break
		}
	}
	if total != 5 {
		t.Fatalf("read %d events, want 5", total)
	}
}

func TestChangesHidePricesAndSkipUpdatesThatOnlyTouchPrices(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1",
		`{"id":1,"nombre":"A","precio_venta":10,"estado":"OK"}`,
		`{"id":2,"nombre":"B","precio_venta":20,"estado":"OK"}`,
		`{"id":3,"nombre":"C","precio_venta":30,"estado":"OK"}`)
	e.publish(t, "v2",
		`{"id":1,"nombre":"A","precio_venta":11,"estado":"OK"}`,  // only the price changed
		`{"id":2,"nombre":"B2","precio_venta":21,"estado":"OK"}`, // name and price changed
		`{"id":4,"nombre":"D","precio_venta":40,"estado":"OK"}`)  // 3 deleted, 4 created

	_, full := e.get(t, "/v1/cambios?desde=0-0", keyFull)
	if len(events(full)) != 4 {
		t.Fatalf("with prices: %v", full)
	}

	_, b := e.get(t, "/v1/cambios?desde=0-0", keyNoCost)
	byID := map[string]map[string]any{}
	for _, x := range events(b) {
		byID[x["id"].(string)] = x
	}
	if _, ok := byID["1"]; ok {
		t.Error("an update that only touched the price must not reach a consumer without prices")
	}
	if len(byID) != 3 {
		t.Fatalf("events for rows 2, 3 and 4 expected, got %v", byID)
	}
	mod := byID["2"]
	if got := mod["campos"].([]any); len(got) != 1 || got[0] != "nombre" {
		t.Errorf("campos = %v", mod["campos"])
	}
	for _, field := range []string{"antes", "fila"} {
		if _, has := mod[field].(map[string]any)["precio_venta"]; has {
			t.Errorf("%s leaks the price: %v", field, mod[field])
		}
	}
	for _, id := range []string{"3", "4"} {
		if _, has := byID[id]["fila"].(map[string]any)["precio_venta"]; has {
			t.Errorf("row %s event leaks the price: %v", id, byID[id])
		}
	}
	raw, _ := json.Marshal(b)
	if strings.Contains(string(raw), "precio_venta") {
		t.Errorf("the response mentions the price field: %s", raw)
	}
}

func TestChangesProgressEvenWhenEverythingWasFiltered(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", `{"id":1,"nombre":"A","precio_venta":10}`)
	e.publish(t, "v2", `{"id":1,"nombre":"A","precio_venta":11}`)
	_, b := e.get(t, "/v1/cambios?desde=0-0", keyNoCost)
	if len(events(b)) != 0 || b["siguiente"] == "0-0" {
		t.Fatalf("the cursor must move past filtered events or the client polls them forever: %v", b)
	}
}

func TestChangesCursorThatFellOffTheStreamIsGone(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:1]...)
	e.publish(t, "v2", baseRows[1:]...) // 5 events
	_, b := e.get(t, "/v1/cambios?desde=0-0&limit=1", keyFull)
	old := b["siguiente"].(string)

	if err := e.rdb.XTrimMaxLen(context.Background(), e.p.ChangesStream(), 1).Err(); err != nil {
		t.Fatal(err)
	}
	code, b := e.get(t, "/v1/cambios?desde="+old, keyFull)
	if code != http.StatusGone || b["perdido"] != true {
		t.Fatalf("%d %v", code, b)
	}
	if code, b = e.get(t, "/v1/cambios?desde=0-0", keyFull); code != http.StatusGone || b["perdido"] != true {
		t.Errorf("0-0 on a trimmed stream also lost the oldest entries: %d %v", code, b)
	}
}

func TestChangesNeedAWellFormedCursor(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	for _, q := range []string{"", "desde=", "desde=abc", "desde=1-", "desde=0-0&limit=0", "desde=0-0&limit=5000"} {
		if code, _ := e.get(t, "/v1/cambios?"+q, keyFull); code != http.StatusBadRequest {
			t.Errorf("%q: status %d, want 400", q, code)
		}
	}
}

func TestOnlyReadMethodsAreAllowed(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req, _ := http.NewRequest(m, e.srv.URL+"/v1/productos/1", nil)
		req.Header.Set("X-API-Key", keyFull)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", m, resp.StatusCode)
		}
	}
}

func TestNewRefusesToServeRestrictedConsumersWithoutPriceFields(t *testing.T) {
	_, err := New(&fakeSearch{}, Options{
		Consumers: []Consumer{{Name: "inventario", KeySHA256: hashOf("k"), Precios: false}},
	})
	if err == nil || !strings.Contains(err.Error(), "NT_PRICE_FIELDS") {
		t.Fatalf("expected an NT_PRICE_FIELDS error, got %v", err)
	}
	if _, err := New(&fakeSearch{}, Options{Consumers: []Consumer{{Name: "x", KeySHA256: hashOf("k"), Precios: true}}}); err != nil {
		t.Errorf("only consumers with prices need no price fields: %v", err)
	}
	if _, err := New(&fakeSearch{}, Options{}); err == nil {
		t.Error("a server without consumers must not start")
	}
}

// --- findings of the independent review ---------------------------------------------------

func TestPriceFieldsAreHiddenWhateverTheirCaseOrDepth(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1",
		`{"id":1,"nombre":"A","valor":10.50,"Precio_Venta":99,"PRECIO_VENTA":98,"x":{"precio_venta":5},"lista":[{"Precio_Venta":1,"ok":2}]}`,
		`{"id":2,"nombre":"B","Precio_Venta":7}`)
	e.publish(t, "v2",
		`{"id":1,"nombre":"A2","valor":10.50,"Precio_Venta":100,"PRECIO_VENTA":98,"x":{"precio_venta":6},"lista":[{"Precio_Venta":1,"ok":2}]}`)

	for _, path := range []string{"/v1/productos?limit=100", "/v1/productos/1", "/v1/productos?estado=NONE", "/v1/cambios?desde=0-0"} {
		code, raw := e.getRaw(t, path, keyNoCost)
		if code != 200 {
			t.Fatalf("%s: %d %s", path, code, raw)
		}
		if strings.Contains(strings.ToLower(raw), "precio_venta") {
			t.Errorf("%s leaks a price field: %s", path, raw)
		}
	}

	_, raw := e.getRaw(t, "/v1/productos/1", keyNoCost)
	if !strings.Contains(raw, `"valor":10.50`) || !strings.Contains(raw, `"ok":2`) || !strings.Contains(raw, `"nombre":"A2"`) { // /v1/productos/1 is the row of v2
		t.Errorf("filtering must keep the other columns and number formatting as published: %s", raw)
	}
	_, raw = e.getRaw(t, "/v1/productos/1", keyFull)
	if !strings.Contains(raw, "Precio_Venta") || !strings.Contains(raw, `"precio_venta":6`) {
		t.Errorf("a consumer with prices gets the row untouched: %s", raw)
	}
}

func TestChangesCursorOfAnEmptyStreamNoticesWhatTrimmingRemovedLater(t *testing.T) {
	e := newEnv(t, envOpts{changesMax: 3})
	e.publish(t, "v1", `{"id":1,"a":"0"}`)
	_, v := e.get(t, "/v1/version", keyFull)
	if v["ultimo_cambio"] != "0-0" {
		t.Fatalf("setup: %v", v)
	}
	for i := 1; i <= 10; i++ { // about 10 events, only 3 survive
		e.publish(t, "v"+strings.Repeat("x", i), `{"id":1,"a":"`+strings.Repeat("y", i)+`"}`)
	}
	if n := e.rdb.XLen(context.Background(), e.p.ChangesStream()).Val(); n != 3 {
		t.Fatalf("setup: the stream holds %d entries, want 3", n)
	}
	code, b := e.get(t, "/v1/cambios?desde=0-0&limit=1000", keyFull)
	if code != http.StatusGone || b["perdido"] != true {
		t.Fatalf("a client that loaded everything while the stream was empty lost events: %d %v", code, b)
	}
}

func TestVersionReportsTheChangeCursorOfTheCurrentVersionNotTheTailOfTheStream(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:2]...)
	e.publish(t, "v2", baseRows[:3]...)
	ctx := context.Background()
	endOfV2 := e.rdb.XRevRangeN(ctx, e.p.ChangesStream(), "+", "-", 1).Val()[0].ID

	// a publication of v3 is in progress: its events are written before nt:actual switches
	e.rdb.XAdd(ctx, &redis.XAddArgs{Stream: e.p.ChangesStream(), Values: []any{
		"tipo", "creado", "id", "99", "version", "v3", "anterior", "v2", "fila", `{"id":99}`}})

	_, v := e.get(t, "/v1/version", keyFull)
	if v["version"] != "v2" || v["ultimo_cambio"] != endOfV2 {
		t.Fatalf("version v2 must come with the cursor of v2 (%s), got %v", endOfV2, v)
	}
	// a consumer holding v2 and that cursor does receive the in-progress v3 event
	_, b := e.get(t, "/v1/cambios?desde="+endOfV2, keyFull)
	if got := events(b); len(got) != 1 || got[0]["id"] != "99" {
		t.Errorf("events after the v2 cursor = %v", b)
	}
}

func TestChangeCursorWithLeadingZerosIsNormalized(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:1]...)
	e.publish(t, "v2", baseRows[1:3]...)
	_, b := e.get(t, "/v1/cambios?desde=0-0", keyFull)
	last := b["siguiente"].(string)
	ms, seq, _ := strings.Cut(last, "-")
	code, b := e.get(t, "/v1/cambios?desde=000"+ms+"-00"+seq, keyFull)
	if code != 200 || len(events(b)) != 0 || b["siguiente"] != last {
		t.Fatalf("the same cursor written with leading zeros must behave the same: %d %v", code, b)
	}
}

func TestACursorCannotNameAVersionThatWasNeverPublished(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	// a half-written version: rows are there but it was never added to nt:versiones nor made current
	e.rdb.HSet(context.Background(), "nt:v9:filas", "1", `{"id":1,"nombre":"DRAFT"}`)
	if code, _ := e.get(t, "/v1/productos?cursor="+url.QueryEscape(encodeCursor("v9", 0, 0)), keyFull); code != http.StatusGone {
		t.Errorf("status %d, want 410", code)
	}
	if code, _ := e.get(t, "/v1/productos?cursor="+url.QueryEscape(encodeCursor("v1", 0, 0)), keyFull); code != 200 {
		t.Errorf("a published version must stay readable: %d", code)
	}
}

type gate struct {
	Source
	started chan struct{}
	release chan struct{}
}

func (g *gate) Meta(ctx context.Context) (publish.Meta, error) {
	g.started <- struct{}{}
	<-g.release
	return g.Source.Meta(ctx)
}

func TestTooManyRequestsInFlightAreRefused(t *testing.T) {
	g := &gate{started: make(chan struct{}, 4), release: make(chan struct{})}
	e := newEnv(t, envOpts{maxInFlight: 1, wrap: func(s Source) Source { g.Source = s; return g }})
	e.publish(t, "v1", baseRows...)

	first := make(chan int, 1)
	go func() { code, _ := e.get(t, "/v1/version", keyFull); first <- code }()
	<-g.started // the first request is now inside the handler holding the only slot

	if code, _ := e.get(t, "/v1/productos", keyFull); code != http.StatusServiceUnavailable {
		t.Errorf("second concurrent request: status %d, want 503", code)
	}
	if code, _ := e.get(t, "/v1/productos", ""); code != http.StatusUnauthorized {
		t.Errorf("a request without key must still be 401, not 503: %d", code)
	}
	close(g.release)
	if code := <-first; code != 200 {
		t.Errorf("the first request must finish normally, got %d", code)
	}
	if code, _ := e.get(t, "/v1/productos", keyFull); code != 200 {
		t.Errorf("the slot must be free again, got %d", code)
	}
}

// --- /healthz -----------------------------------------------------------------------------

func TestHealthzNeedsNoKeyAndExposesNothingButTheStatus(t *testing.T) {
	e := newEnv(t, envOpts{}) // nothing published: the process and Redis are fine, that is all it reports
	code, raw := e.getRaw(t, "/healthz", "")
	if code != http.StatusOK || strings.TrimSpace(raw) != `{"status":"ok"}` {
		t.Fatalf("%d %q", code, raw)
	}
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/healthz", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("Cache-Control") != "no-store" || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("headers = %v", resp.Header)
	}
	// being open does not open anything else
	if code, _ := e.get(t, "/v1/version", ""); code != http.StatusUnauthorized {
		t.Errorf("/v1/version without a key: %d, want 401", code)
	}
	if code, _ := e.get(t, "/healthz/extra", ""); code != http.StatusUnauthorized {
		t.Errorf("only /healthz itself is public: %d", code)
	}
}

func TestHealthzReportsRedisDownWithoutLeakingTheError(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.mr.Close()
	code, raw := e.getRaw(t, "/healthz", "")
	if code != http.StatusServiceUnavailable || strings.TrimSpace(raw) != `{"status":"unavailable"}` {
		t.Fatalf("%d %q", code, raw)
	}
}

func TestHealthzDoesNotTakeARequestSlot(t *testing.T) {
	g := &gate{started: make(chan struct{}, 4), release: make(chan struct{})}
	e := newEnv(t, envOpts{maxInFlight: 1, wrap: func(s Source) Source { g.Source = s; return g }})
	e.publish(t, "v1", baseRows...)

	first := make(chan int, 1)
	go func() { code, _ := e.get(t, "/v1/version", keyFull); first <- code }()
	<-g.started // the only slot is taken

	if code, _ := e.getRaw(t, "/healthz", ""); code != http.StatusOK {
		t.Errorf("a monitor must still get an answer when the API is busy: %d", code)
	}
	close(g.release)
	<-first
}

func TestHealthzIsNotAShortcutIntoTheAuthenticatedRoutes(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	for _, path := range []string{
		"/healthz/", "/HEALTHZ", "/healthz/../v1/version", "/healthz/%2e%2e/v1/version",
		"/healthz%2f..%2fv1%2fversion", "/healthz/../v1/productos", "//v1/version", "/./v1/version",
	} {
		code, raw := e.getRaw(t, path, "")
		if strings.Contains(raw, `"version":"v1"`) || strings.Contains(raw, "productos") || strings.Contains(raw, "LEVOFLOXACINO") {
			t.Errorf("%s leaked data without a key (status %d): %s", path, code, raw)
		}
	}
}

// --- the API returns only the columns it was told to ---------------------------------------

var wideRow = `{"id":1,"cum_facturar":"123-1","nombre":"A","estado":"OK","cod_auf":"101","precio_venta":10,"precio_maximo":12,` +
	`"contrato Evento":"SI","Nombre":"mayus","datos_fuente":{"atc":"X","precio_maximo":9},"motivos":[{"codigo":"NT24"}]}`

func keysOf(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestOnlyTheConfiguredColumnsAreReturned(t *testing.T) {
	e := newEnv(t, envOpts{
		fields:      []string{"id", "cum_facturar", "nombre", "contrato Evento", "precio_maximo"},
		priceFields: []string{"precio_venta", "precio_maximo"},
	})
	e.publish(t, "v1", wideRow)

	want := func(key string, cols ...string) {
		t.Helper()
		for _, path := range []string{"/v1/productos/1", "/v1/productos?cum_facturar=123-1", "/v1/productos?limit=10"} {
			code, body := e.get(t, path, key)
			if code != 200 {
				t.Fatalf("%s: %d %v", path, code, body)
			}
			row := body
			if _, list := body["productos"]; list {
				row = items(body)[0]
			}
			// id_fila (the row key) is the only column that is not on the list: it is added to every product
			got := slices.DeleteFunc(keysOf(row), func(k string) bool { return k == rowKeyField })
			if _, ok := row[rowKeyField]; !ok {
				t.Errorf("%s as %s lacks the row key", path, key)
			}
			if !slices.Equal(got, cols) {
				t.Errorf("%s as %s returned columns %v, want %v", path, key, got, cols)
			}
		}
	}
	want(keyFull, "contrato Evento", "cum_facturar", "id", "nombre", "precio_maximo")
	want(keyNoCost, "contrato Evento", "cum_facturar", "id", "nombre") // the price rule still applies to the allowed price column
}

func TestColumnsAreMatchedExactlyAndNestedOnesAreNotExposedByTheirParentsName(t *testing.T) {
	e := newEnv(t, envOpts{fields: []string{"id", "nombre"}})
	e.publish(t, "v1", wideRow)
	_, raw := e.getRaw(t, "/v1/productos/1", keyFull)
	if strings.Contains(raw, "mayus") || strings.Contains(raw, "datos_fuente") || strings.Contains(raw, "motivos") {
		t.Errorf("a column that is not on the list leaked (matching must be exact, \"Nombre\" is not \"nombre\"): %s", raw)
	}
}

func TestAllowedColumnsKeepTheirValuesAsPublished(t *testing.T) {
	e := newEnv(t, envOpts{fields: []string{"id", "valor", "lista", "txt"}})
	e.publish(t, "v1", `{"id":1,"valor":10.50,"lista":[{"a":1.0}],"txt":"ñ","otro":1}`)
	for _, key := range []string{keyFull, keyNoCost} {
		_, raw := e.getRaw(t, "/v1/productos/1", key)
		for _, frag := range []string{`"valor":10.50`, `"a":1.0`, `"txt":"ñ"`} {
			if !strings.Contains(raw, frag) {
				t.Errorf("%s: %s missing in %s", key, frag, raw)
			}
		}
	}
}

func TestChangesCarryOnlyTheConfiguredColumns(t *testing.T) {
	e := newEnv(t, envOpts{fields: []string{"id", "nombre"}})
	e.publish(t, "v1",
		`{"id":1,"nombre":"A","estado":"OK"}`,
		`{"id":2,"nombre":"B","estado":"OK"}`)
	e.publish(t, "v2",
		`{"id":1,"nombre":"A","estado":"ERROR"}`, // only a hidden column changed
		`{"id":2,"nombre":"B2","estado":"ERROR"}`)

	for _, key := range []string{keyFull, keyNoCost} {
		_, body := e.get(t, "/v1/cambios?desde=0-0", key)
		byID := map[string]map[string]any{}
		for _, x := range events(body) {
			byID[x["id"].(string)] = x
		}
		if _, ok := byID["1"]; ok {
			t.Errorf("%s: an update that only touched a column the API does not return must not be delivered", key)
		}
		mod := byID["2"]
		if mod == nil {
			t.Fatalf("%s: the update of row 2 is missing: %v", key, body)
		}
		if got := mod["campos"].([]any); len(got) != 1 || got[0] != "nombre" {
			t.Errorf("%s: campos = %v", key, mod["campos"])
		}
		// "antes" holds only the values the update replaced, "fila" the whole new row
		if got := keysOf(mod["antes"].(map[string]any)); !slices.Equal(got, []string{"nombre"}) {
			t.Errorf("%s: antes has columns %v", key, got)
		}
		if got := keysOf(mod["fila"].(map[string]any)); !slices.Equal(got, []string{"id", "nombre"}) {
			t.Errorf("%s: fila has columns %v", key, got)
		}
		if strings.Contains(jsonString(body), "estado") {
			t.Errorf("%s: the response mentions a column that is not returned: %v", key, body)
		}
		if body["siguiente"] == "0-0" {
			t.Errorf("%s: the cursor must move past the update nobody receives", key)
		}
	}
}

func TestNewRefusesAPriceColumnThatConsumersWithoutPricesWouldSee(t *testing.T) {
	restricted := []Consumer{{Name: "inventario", KeySHA256: hashOf("k"), Precios: false}}
	_, err := New(&fakeSearch{}, Options{Consumers: restricted, PriceFields: []string{"precio_venta"}, Fields: []string{"id", "Precio_Maximo"}})
	if err == nil || !strings.Contains(err.Error(), "NT_PRICE_FIELDS") || !strings.Contains(err.Error(), "Precio_Maximo") {
		t.Fatalf("a price column missing from NT_PRICE_FIELDS must stop the server, got %v", err)
	}
	if _, err := New(&fakeSearch{}, Options{Consumers: restricted, PriceFields: []string{"precio_venta", "precio_maximo"}, Fields: []string{"id", "Precio_Maximo"}}); err != nil {
		t.Errorf("a price column listed in NT_PRICE_FIELDS is fine: %v", err)
	}
	full := []Consumer{{Name: "x", KeySHA256: hashOf("k"), Precios: true}}
	if _, err := New(&fakeSearch{}, Options{Consumers: full, Fields: []string{"id", "precio_maximo"}}); err != nil {
		t.Errorf("when everybody may see prices nothing has to be hidden: %v", err)
	}
}

// --- filters and search cannot probe a column the API does not return ---------------------

func TestFiltersAndSearchOnlyUseTheReturnedColumns(t *testing.T) {
	e := newEnv(t, envOpts{
		fields:       []string{"id", "cum_facturar", "nombre"},
		searchFields: []string{"nombre", "estado"},
	})
	e.publish(t, "v1", baseRows...)

	for _, key := range []string{keyFull, keyNoCost} {
		code, body := e.get(t, "/v1/productos?estado=ERROR", key)
		if code != http.StatusBadRequest {
			t.Errorf("%s: filtering by a column that is not returned: status %d, want 400: %v", key, code, body)
		}
		if strings.Contains(jsonString(body), "estado") || strings.Contains(jsonString(body), "precio_venta") {
			t.Errorf("%s: the error names a column that is not returned: %v", key, body)
		}
		if code, body := e.get(t, "/v1/productos?cum_facturar=123-1", key); code != 200 || len(items(body)) != 1 {
			t.Errorf("%s: a returned column still filters: %d %v", key, code, body)
		}
		if code, _ := e.get(t, "/v1/productos?q=acido", key); code != 200 || e.fs.gotQuery != "@nombre:(*acido*)" {
			t.Errorf("%s: search must use only the returned columns, query = %q (status %d)", key, e.fs.gotQuery, code)
		}
	}

	e = newEnv(t, envOpts{fields: []string{"id"}, searchFields: []string{"estado"}})
	e.publish(t, "v1", baseRows...)
	if code, _ := e.get(t, "/v1/productos?q=ok", keyFull); code != http.StatusForbidden {
		t.Errorf("a search over columns that are not returned must be refused, got %d", code)
	}
}

func TestEventFieldNamesAreFilteredWhateverTheKind(t *testing.T) {
	s := &server{fields: set([]string{"id", "nombre"}), priceLC: lowerSet([]string{"precio_maximo"})}
	for _, c := range []Consumer{{Name: "a", Precios: true}, {Name: "b", Precios: false}} {
		v, ok, err := s.eventFor(c, publish.ChangeEvent{Kind: "creado", ID: "1", Fields: []string{"estado", "precio_maximo", "nombre"}, Row: json.RawMessage(`{"id":1,"estado":"X"}`)})
		if err != nil || !ok {
			t.Fatalf("%s: a created row is always delivered: %v %v", c.Name, ok, err)
		}
		if strings.Contains(strings.Join(v.Campos, ","), "estado") || strings.Contains(strings.Join(v.Campos, ","), "precio_maximo") {
			t.Errorf("%s: campos names a column that is not visible: %v", c.Name, v.Campos)
		}
	}
}

// --- the columns the billing app (Tessia) needs ----------------------------------------------

func defaultConfig(t *testing.T) config.Config {
	t.Helper()
	env := map[string]string{"NT_API_URL": "https://h/api.php", "NT_API_KEY": "k", "REDIS_ADDR": "r:6379"}
	c, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const billingRow = `{"id":7,"cum_facturar":"900-1","cod_auf":"1957","estado":"ACTIVO","clasificacion":"B","descripcion":"ACIDO",` +
	`"precio_venta":10,"precio_maximo":12,"precio_final":11.5,"precio_contrato":9,"precio_digitado":3,"origen_precio":"X"}`

func TestTheDefaultColumnsServeTheBillingFieldsAndStillHidePricesFromRestrictedKeys(t *testing.T) {
	cfg := defaultConfig(t)
	e := newEnv(t, envOpts{fields: cfg.HTTP.Fields, priceFields: cfg.HTTP.PriceFields, indexFields: cfg.IndexFields})
	e.publish(t, "v1", billingRow)

	_, full := e.get(t, "/v1/productos/7", keyFull)
	for _, col := range []string{"precio_final", "precio_contrato", "precio_maximo", "estado", "clasificacion", "cod_auf"} {
		if _, ok := full[col]; !ok {
			t.Errorf("a project that may see prices lacks %q: %v", col, keysOf(full))
		}
	}
	if full["precio_final"] != 11.5 || full["precio_contrato"] != float64(9) {
		t.Errorf("price values changed: %v", full)
	}
	for _, col := range []string{"precio_digitado", "origen_precio"} {
		if _, ok := full[col]; ok {
			t.Errorf("%q is not one of the agreed columns but was returned", col)
		}
	}

	for _, path := range []string{"/v1/productos/7", "/v1/productos?cod_auf=1957", "/v1/productos?estado=activo", "/v1/productos?limit=5"} {
		code, body := e.get(t, path, keyNoCost)
		if code != 200 {
			t.Fatalf("%s: %d %v", path, code, body)
		}
		row := body
		if _, list := body["productos"]; list {
			row = items(body)[0]
		}
		for col := range row {
			if strings.Contains(strings.ToLower(col), "precio") {
				t.Errorf("%s: a project without prices received %q", path, col)
			}
		}
		for _, col := range []string{"estado", "clasificacion", "cod_auf"} {
			if _, ok := row[col]; !ok {
				t.Errorf("%s: a project without prices lacks %q", path, col)
			}
		}
	}
	if code, _ := e.get(t, "/v1/productos?precio_final=11.5", keyNoCost); code == 200 {
		t.Error("a project without prices must not filter by a price column")
	}
}

func TestTheServerRefusesTheDefaultColumnsWithAnOldPriceList(t *testing.T) {
	// A .env written before precio_final and precio_contrato were returned lists only the older price
	// fields: starting would hand those two columns to the projects that may not see prices.
	cfg := defaultConfig(t)
	_, err := New(nil, Options{
		Consumers:   []Consumer{{Name: "inventario", KeySHA256: hashOf(keyNoCost)}},
		PriceFields: []string{"precio_venta", "precio_maximo"},
		Fields:      cfg.HTTP.Fields,
	})
	if err == nil || !strings.Contains(err.Error(), "precio_final") {
		t.Fatalf("expected a refusal naming precio_final, got %v", err)
	}
	if _, err := New(nil, Options{
		Consumers:   []Consumer{{Name: "inventario", KeySHA256: hashOf(keyNoCost)}},
		PriceFields: cfg.HTTP.PriceFields,
		Fields:      cfg.HTTP.Fields,
	}); err != nil {
		t.Fatalf("the shipped defaults must start: %v", err)
	}
}

// --- versions: GET /v1/versiones and ?version= -------------------------------------------------

func TestVersionsListsTheKeptVersionsOldestFirst(t *testing.T) {
	e := newEnv(t, envOpts{keep: 3})
	if code, _ := e.get(t, "/v1/versiones", keyFull); code != http.StatusServiceUnavailable {
		t.Errorf("nothing published: status %d, want 503", code)
	}
	e.publish(t, "v1", baseRows...)
	e.publish(t, "v2", baseRows...)
	e.publish(t, "v3", baseRows...)
	for _, key := range []string{keyFull, keyNoCost} {
		code, raw := e.getRaw(t, "/v1/versiones", key)
		if want := `{"actual":"v3","versiones":["v1","v2","v3"]}`; code != 200 || strings.TrimSpace(raw) != want {
			t.Errorf("%s: %d %q, want %s", key, code, raw, want)
		}
	}
	if code, _ := e.get(t, "/v1/versiones", ""); code != http.StatusUnauthorized {
		t.Errorf("without a key: status %d, want 401", code)
	}

	e.publish(t, "v4", baseRows...) // keep=3 drops v1
	if _, raw := e.getRaw(t, "/v1/versiones", keyFull); strings.TrimSpace(raw) != `{"actual":"v4","versiones":["v2","v3","v4"]}` {
		t.Errorf("after pruning: %q", raw)
	}
}

func publishTwoVersions(t *testing.T, e *env) {
	t.Helper()
	e.publish(t, "v1",
		`{"id":1,"cum_facturar":"A-1","nombre":"OLD ONE","precio_venta":10,"estado":"OK"}`,
		`{"id":2,"cum_facturar":"A-2","nombre":"OLD TWO","precio_venta":20,"estado":"OK"}`)
	e.publish(t, "v2",
		`{"id":1,"cum_facturar":"A-1","nombre":"NEW ONE","precio_venta":11,"estado":"OK"}`,
		`{"id":2,"cum_facturar":"A-2","nombre":"NEW TWO","precio_venta":21,"estado":"OK"}`,
		`{"id":3,"cum_facturar":"A-3","nombre":"NEW THREE","precio_venta":31,"estado":"OK"}`)
}

func TestVersionParamReadsThatExactVersionOnEveryEndpoint(t *testing.T) {
	e := newEnv(t, envOpts{searchFields: []string{"nombre"}})
	publishTwoVersions(t, e)
	e.fs.ids = []string{"1"}

	for _, key := range []string{keyFull, keyNoCost} {
		// the current version without the parameter, exactly as before
		_, b := e.get(t, "/v1/productos?cum_facturar=A-1", key)
		if b["version"] != "v2" || items(b)[0]["nombre"] != "NEW ONE" {
			t.Errorf("%s: no parameter must read the current version: %v", key, b)
		}

		code, b := e.get(t, "/v1/productos?limit=10&version=v1", key)
		if code != 200 || b["version"] != "v1" || len(items(b)) != 2 {
			t.Errorf("%s list: %d %v", key, code, b)
		}
		code, b = e.get(t, "/v1/productos/1?version=v1", key)
		if code != 200 || b["nombre"] != "OLD ONE" {
			t.Errorf("%s one product: %d %v", key, code, b)
		}
		if code, b = e.get(t, "/v1/productos/3?version=v1", key); code != http.StatusNotFound {
			t.Errorf("%s: a product that only exists in v2 is not in v1: %d %v", key, code, b)
		}
		code, b = e.get(t, "/v1/productos?cum_facturar=A-1&version=v1", key)
		if code != 200 || b["version"] != "v1" || b["total"] != float64(1) || items(b)[0]["nombre"] != "OLD ONE" {
			t.Errorf("%s exact filter: %d %v", key, code, b)
		}
		e.fs.gotVer = ""
		code, b = e.get(t, "/v1/productos?q=one&version=v1", key)
		if code != 200 || b["version"] != "v1" || e.fs.gotVer != "v1" || items(b)[0]["nombre"] != "OLD ONE" {
			t.Errorf("%s text search: %d %v (searched version %q)", key, code, b, e.fs.gotVer)
		}
		code, b = e.get(t, "/v1/version?version=v1", key)
		if code != 200 || b["version"] != "v1" || b["total"] != float64(2) {
			t.Errorf("%s version: %d %v", key, code, b)
		}
		if code, b = e.get(t, "/v1/version?version=v2", key); code != 200 || b["version"] != "v2" || b["total"] != float64(3) {
			t.Errorf("%s the current version can be named too: %d %v", key, code, b)
		}
	}
	// the price rule does not depend on the version that is read
	_, raw := e.getRaw(t, "/v1/productos?limit=10&version=v1", keyNoCost)
	if strings.Contains(raw, "precio_venta") {
		t.Errorf("a pinned read leaked a price: %s", raw)
	}
	if _, raw = e.getRaw(t, "/v1/productos/1?version=v1", keyFull); !strings.Contains(raw, `"precio_venta":10`) {
		t.Errorf("a project with prices lost them on a pinned read: %s", raw)
	}
}

func TestPinnedVersionStartCursorOfTheChangesBelongsToThatVersion(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:2]...)
	e.publish(t, "v2", baseRows...)
	_, v1 := e.get(t, "/v1/version?version=v1", keyFull)
	_, v2 := e.get(t, "/v1/version?version=v2", keyFull)
	if v1["ultimo_cambio"] == v2["ultimo_cambio"] || v1["ultimo_cambio"] == nil {
		t.Errorf("each version reports the change cursor recorded when it was published: v1=%v v2=%v", v1["ultimo_cambio"], v2["ultimo_cambio"])
	}
}

func TestAnUnknownOrRemovedVersionIs404OnEveryEndpoint(t *testing.T) {
	e := newEnv(t, envOpts{keep: 1, searchFields: []string{"nombre"}})
	e.publish(t, "v1", baseRows...)
	e.publish(t, "v2", baseRows...) // keep=1: v1 is removed
	// a half-written version: rows exist but it was never listed in nt:versiones
	e.rdb.HSet(context.Background(), "nt:v9:filas", "1", `{"id":1,"nombre":"DRAFT"}`)

	for _, ver := range []string{"v1", "nope", "v9", "a%2Fb", "..%2Fx", strings.Repeat("a", 300)} {
		for _, path := range []string{"/v1/productos?version=", "/v1/productos/1?version=", "/v1/version?version=",
			"/v1/productos?estado=OK&version=", "/v1/productos?q=acido&version="} {
			code, b := e.get(t, path+ver, keyFull)
			if code != http.StatusNotFound || b["error"] == nil {
				t.Errorf("%s%s: %d %v, want 404 with an error", path, ver, code, b)
			}
		}
	}
}

func TestAnEmptyOrRepeatedVersionParameterIsRejected(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	for _, q := range []string{"version=", "version=v1&version=v1", "version=v1&version="} {
		// reading the current version when the caller meant to pin one would be silent and wrong
		if code, _ := e.get(t, "/v1/productos?"+q, keyFull); code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, code)
		}
	}
}

func TestVersionAndCursorMustAgree(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	e.publish(t, "v2", baseRows...)
	_, first := e.get(t, "/v1/productos?limit=2&version=v1", keyFull)
	next, _ := first["siguiente"].(string)
	if first["version"] != "v1" || next == "" {
		t.Fatalf("%v", first)
	}
	if code, _ := e.get(t, "/v1/productos?version=v2&cursor="+url.QueryEscape(next), keyFull); code != http.StatusBadRequest {
		t.Errorf("version and cursor of different versions: status %d, want 400", code)
	}
	code, b := e.get(t, "/v1/productos?limit=100&version=v1&cursor="+url.QueryEscape(next), keyFull)
	if code != 200 || b["version"] != "v1" || len(items(b)) != 3 {
		t.Errorf("the same version continues the read: %d %v", code, b)
	}
	// without version the cursor still decides, as before
	if code, b = e.get(t, "/v1/productos?limit=100&cursor="+url.QueryEscape(next), keyFull); code != 200 || b["version"] != "v1" {
		t.Errorf("a cursor alone: %d %v", code, b)
	}
}

func TestTheChangeStreamIgnoresTheVersionParameter(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows...)
	if code, _ := e.get(t, "/v1/cambios?desde=0-0&version=nope", keyFull); code != 200 {
		t.Errorf("status %d, want 200", code)
	}
}

// spy wraps the Source of a test to count its calls, see their deadlines and react after a lookup.
type spy struct {
	Source
	mu          sync.Mutex
	calls       int
	deadlines   []time.Duration // time left on the request context at each call; 0 when it had none
	afterLookup func()
	infinite    bool // claims every variant key exists
}

func (s *spy) note(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if d, ok := ctx.Deadline(); ok {
		s.deadlines = append(s.deadlines, time.Until(d))
	} else {
		s.deadlines = append(s.deadlines, 0)
	}
}

func (s *spy) Lookup(ctx context.Context, ver, field, value string) ([]string, error) {
	s.note(ctx)
	ids, err := s.Source.Lookup(ctx, ver, field, value)
	if s.afterLookup != nil {
		s.afterLookup()
	}
	return ids, err
}

func (s *spy) RowsByID(ctx context.Context, ver string, ids []string) ([]publish.Raw, error) {
	s.note(ctx)
	if !s.infinite {
		return s.Source.RowsByID(ctx, ver, ids)
	}
	var out []publish.Raw
	for _, id := range ids {
		if strings.Contains(id, "#") {
			out = append(out, publish.Raw{ID: id, JSON: json.RawMessage(`{"id":1}`)})
		}
	}
	return out, nil
}

func TestAPinnedVersionRemovedWhileItIsBeingReadIsNotServedPartially(t *testing.T) {
	sp := &spy{}
	e := newEnv(t, envOpts{keep: 2, wrap: func(s Source) Source { sp.Source = s; return sp }})
	e.publish(t, "v1", baseRows...)
	e.publish(t, "v2", baseRows...)
	// a publication that prunes v1 lands between the lookup and the rows being read
	sp.afterLookup = func() { sp.afterLookup = nil; e.publish(t, "v3", baseRows...) }
	code, b := e.get(t, "/v1/productos?estado=OK&version=v1", keyFull)
	if code != http.StatusNotFound {
		t.Errorf("status %d %v, want 404 rather than a 200 built on a version that went away", code, b)
	}
}

// --- exact filter contract ---------------------------------------------------------------------

func rowKeys(body map[string]any) []string {
	var out []string
	for _, r := range items(body) {
		k, _ := r["id_fila"].(string)
		out = append(out, k)
	}
	return out
}

var familyRows = []string{
	`{"id":9,"cum_facturar":"F-9","plu":"a"}`,
	`{"id":9,"cum_facturar":"F-9","plu":"b"}`,
	`{"id":10,"cum_facturar":"OTHER","plu":"c"}`,
	`{"id":10,"cum_facturar":"F-9","plu":"d"}`,
	`{"id":10,"plu":"e"}`,
	`{"id":11,"cum_facturar":"NOPE","plu":"f"}`,
}

func TestExactFilterReturnsEveryRowOfTheLineInBaseIDThenVariantOrder(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", familyRows...)
	want := []string{"9", "9#2", "10", "10#2", "10#3"}

	for _, key := range []string{keyFull, keyNoCost} {
		code, b := e.get(t, "/v1/productos?cum_facturar="+url.QueryEscape("  f-9 ")+"&version=v1", key)
		if code != 200 || b["version"] != "v1" || b["total"] != float64(5) || b["siguiente"] != "" {
			t.Fatalf("%s: %d %v", key, code, b)
		}
		// 10#2 matches but 10 does not: the whole family comes along; 10 sorts after 9 (numeric id), not before it
		if got := rowKeys(b); !slices.Equal(got, want) {
			t.Errorf("%s: row keys %v, want %v", key, got, want)
		}
		for i, r := range items(b) {
			if r["plu"] == nil || (i < 2 && r["id"] != float64(9)) || (i >= 2 && r["id"] != float64(10)) {
				t.Errorf("%s: the id column must keep what was published: %v", key, r)
			}
		}
	}

	// paging walks the same order, and total always counts the whole family
	var all []string
	path := "/v1/productos?cum_facturar=F-9&limit=2"
	for pages := 0; pages < 6; pages++ {
		_, b := e.get(t, path, keyFull)
		if b["total"] != float64(5) {
			t.Fatalf("total %v on %s", b["total"], path)
		}
		all = append(all, rowKeys(b)...)
		next, _ := b["siguiente"].(string)
		if next == "" {
			break
		}
		path = "/v1/productos?cum_facturar=F-9&limit=2&offset=" + next
	}
	if !slices.Equal(all, want) {
		t.Errorf("paged rows %v, want %v", all, want)
	}
}

func TestExactFilterWithAValueThatMatchesNothingIsAnEmptyList(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", familyRows...)
	code, raw := e.getRaw(t, "/v1/productos?cum_facturar=NO-SUCH&version=v1", keyFull)
	var b struct {
		Version   string `json:"version"`
		Total     *int   `json:"total"`
		Productos []any  `json:"productos"`
		Siguiente string `json:"siguiente"`
	}
	if err := json.Unmarshal([]byte(raw), &b); err != nil || code != 200 || b.Version != "v1" || b.Total == nil || *b.Total != 0 ||
		b.Productos == nil || len(b.Productos) != 0 || b.Siguiente != "" {
		t.Errorf("%d %s", code, raw)
	}
	if !strings.Contains(raw, `"productos":[]`) {
		t.Errorf("productos must be an empty array, not null: %s", raw)
	}
}

func TestExactFilterOrdersVariantsByNumberNotByText(t *testing.T) {
	rows := make([]string, 0, 50)
	for i := 1; i <= 50; i++ {
		cum := ""
		if i == 37 {
			cum = `,"cum_facturar":"BIG"` // only one variant of the line is in the index
		}
		rows = append(rows, fmt.Sprintf(`{"id":1,"plu":"p%d"%s}`, i, cum))
	}
	sp := &spy{}
	e := newEnv(t, envOpts{wrap: func(s Source) Source { sp.Source = s; return sp }})
	e.publish(t, "v1", rows...)

	start := time.Now()
	code, b := e.get(t, "/v1/productos?cum_facturar=big", keyFull)
	if code != 200 || b["total"] != float64(50) {
		t.Fatalf("%d %v", code, b["total"])
	}
	got := rowKeys(b)
	if len(got) != 50 || got[0] != "1" || got[1] != "1#2" || got[9] != "1#10" || got[49] != "1#50" {
		t.Errorf("order %v", got)
	}

	// a family of 50 rows must not cost one round trip per row, and every call carries the request deadline
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.calls > 12 {
		t.Errorf("%d Redis reads for one filter of 50 rows", sp.calls)
	}
	for _, left := range sp.deadlines {
		if left <= 0 || left > requestTimeout {
			t.Errorf("a Redis read ran with %v left; every read must be bound by the %v request deadline", left, requestTimeout)
		}
	}
	if time.Since(start) > requestTimeout {
		t.Errorf("took %v", time.Since(start))
	}
}

func TestAFamilyThatNeverEndsIsRefusedInsteadOfLoopingUntilTheDeadline(t *testing.T) {
	sp := &spy{infinite: true}
	e := newEnv(t, envOpts{wrap: func(s Source) Source { sp.Source = s; return sp }})
	e.publish(t, "v1", `{"id":1,"cum_facturar":"X"}`)
	start := time.Now()
	code, _ := e.get(t, "/v1/productos?cum_facturar=X", keyFull)
	if code != http.StatusInternalServerError || time.Since(start) > 5*time.Second {
		t.Errorf("status %d after %v, want a quick 500", code, time.Since(start))
	}
}

// --- every product carries its row key -----------------------------------------------------------

func TestEveryProductCarriesItsRowKeyWhateverTheEndpoint(t *testing.T) {
	e := newEnv(t, envOpts{searchFields: []string{"nombre"}, fields: []string{"id", "nombre", "precio_venta", "estado"}})
	e.publish(t, "v1", baseRows...) // two rows share id 3: the second one is the row "3#2"
	e.fs.ids = []string{"3#2"}

	for _, key := range []string{keyFull, keyNoCost} {
		_, one := e.get(t, "/v1/productos/3%232", key)
		if one["id_fila"] != "3#2" || one["id"] != float64(3) {
			t.Errorf("%s one product: %v", key, one)
		}
		_, base := e.get(t, "/v1/productos/1", key)
		if base["id_fila"] != "1" {
			t.Errorf("%s: %v", key, base)
		}
		_, list := e.get(t, "/v1/productos?limit=10", key)
		got := rowKeys(list)
		slices.Sort(got)
		if !slices.Equal(got, []string{"1", "2", "3", "3#2", "5"}) {
			t.Errorf("%s list: %v", key, got)
		}
		if _, f := e.get(t, "/v1/productos?estado=ok", key); !slices.Contains(rowKeys(f), "3#2") {
			t.Errorf("%s filter: %v", key, rowKeys(f))
		}
		if _, q := e.get(t, "/v1/productos?q=gotas", key); !slices.Equal(rowKeys(q), []string{"3#2"}) {
			t.Errorf("%s search: %v", key, rowKeys(q))
		}
	}
}

func TestTheRowKeyIsOursEvenWhenARowBringsAColumnWithThatName(t *testing.T) {
	e := newEnv(t, envOpts{}) // every column, prices allowed: the untouched path
	e.publish(t, "v1", `{"id":1,"valor":10.50,"lista":[1.0],"id_fila":"evil"}`, `{"id":2,"valor":1.0}`)
	_, raw := e.getRaw(t, "/v1/productos/1", keyFull)
	var b map[string]any
	if err := json.Unmarshal([]byte(raw), &b); err != nil || b["id_fila"] != "1" || strings.Count(raw, `"id_fila"`) != 1 {
		t.Errorf("%s", raw)
	}
	_, raw = e.getRaw(t, "/v1/productos/2", keyFull)
	if !strings.Contains(raw, `"valor":1.0`) || !strings.Contains(raw, `"id_fila":"2"`) {
		t.Errorf("a row without that column keeps its numbers as published: %s", raw)
	}
}

func TestChangeEventsDoNotCarryTheRowKeyInsideTheirRow(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.publish(t, "v1", baseRows[:2]...)
	e.publish(t, "v2", baseRows[:3]...)
	_, raw := e.getRaw(t, "/v1/cambios?desde=0-0", keyFull)
	if strings.Contains(raw, "id_fila") || !strings.Contains(raw, `"id":"3"`) {
		t.Errorf("the event already names its row in id: %s", raw)
	}
}
