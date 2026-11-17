package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

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
	ids      []string
	called   bool
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
}

type envOpts struct {
	keep         int
	searchFields []string
	changesMax   int64
	maxInFlight  int
	wrap         func(Source) Source
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
		PriceFields:  []string{"precio_venta"},
		IndexFields:  testIndexFields,
		SearchFields: o.searchFields,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &env{srv: srv, rdb: rdb, p: p, mr: mr, fs: fs}
}

func (e *env) publish(t *testing.T, version string, filas ...string) {
	t.Helper()
	s := model.Snapshot{Version: version, GeneradoEn: "2026-11-17 10:00:00", ConPrecios: true, BasesFaltantes: json.RawMessage(`[]`)}
	for _, f := range filas {
		s.Filas = append(s.Filas, json.RawMessage(f))
	}
	rows, err := s.Rows(testIndexFields)
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
