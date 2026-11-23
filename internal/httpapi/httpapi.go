package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"nt-sync/internal/publish"
)

const (
	defaultLimit = 500
	maxLimit     = 1000
	maxQueryLen  = 100 // characters of the text search
	maxTerms     = 6
	minTermLen   = 2
	maxOffset    = 100000 // RediSearch refuses to page past its own result cap anyway

	defaultInFlight = 64
	requestTimeout  = 20 * time.Second // Redis calls of a request are cancelled after this
	healthTimeout   = 2 * time.Second
)

// Source is the read side of Redis; *publish.Publisher implements it.
type Source interface {
	Ping(ctx context.Context) error
	Current(ctx context.Context) (string, error)
	Meta(ctx context.Context) (publish.Meta, error)
	ChangesEnd(ctx context.Context) (string, error)
	Page(ctx context.Context, ver string, cursor uint64, skip, limit int) ([]publish.Raw, uint64, int, error)
	Row(ctx context.Context, ver, id string) ([]byte, bool, error)
	Lookup(ctx context.Context, ver, field, value string) ([]string, error)
	RowsByID(ctx context.Context, ver string, ids []string) ([]publish.Raw, error)
	SearchIDs(ctx context.Context, query string, offset, limit int) (string, []string, int, error)
	ChangesSince(ctx context.Context, after string, limit int) ([]publish.ChangeEvent, bool, bool, error)
}

// Options configures the API.
type Options struct {
	Consumers    []Consumer
	PriceFields  []string // top-level row fields hidden from consumers without Precios
	Fields       []string // columns the API returns; nil returns every column
	IndexFields  []string // fields that can be used as exact filters
	SearchFields []string // fields behind the text search; empty turns ?q= off
	Log          *slog.Logger
	MaxInFlight  int // requests served at the same time; 0 means the default
}

type server struct {
	src     Source
	cons    []Consumer
	priceLC map[string]bool // lower-case price field names
	fields  map[string]bool // columns that are returned, matched exactly; nil returns every column
	index   map[string]bool
	slots   chan struct{} // one token per request being served
	search  []string
	log     *slog.Logger
}

type consumerKey struct{}

// New builds the handler. Every route requires a valid X-API-Key.
func New(src Source, opt Options) (http.Handler, error) {
	if len(opt.Consumers) == 0 {
		return nil, errors.New("no consumers: the API would be open or unusable")
	}
	inFlight := opt.MaxInFlight
	if inFlight <= 0 {
		inFlight = defaultInFlight
	}
	s := &server{
		src: src, cons: opt.Consumers, priceLC: lowerSet(opt.PriceFields), index: set(opt.IndexFields),
		search: opt.SearchFields, log: opt.Log, slots: make(chan struct{}, inFlight),
	}
	if opt.Fields != nil {
		s.fields = set(opt.Fields)
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	for _, c := range opt.Consumers {
		if !c.Precios && len(opt.PriceFields) == 0 {
			return nil, fmt.Errorf("consumer %q cannot see prices but NT_PRICE_FIELDS is empty: nothing would be hidden", c.Name)
		}
	}
	// A column that looks like a price but is not in NT_PRICE_FIELDS would reach the consumers that may
	// not see prices. Refusing to start is safer than guessing which of them is one.
	for _, c := range opt.Consumers {
		if c.Precios {
			continue
		}
		for _, f := range opt.Fields {
			if strings.Contains(strings.ToLower(f), "precio") && !s.isPrice(f) {
				return nil, fmt.Errorf("column %q looks like a price but is not in NT_PRICE_FIELDS: consumer %q, which cannot see prices, would receive it", f, c.Name)
			}
		}
	}
	for _, f := range opt.SearchFields {
		if !plainName(f) {
			return nil, fmt.Errorf("search field %q is not a plain field name", f)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/version", s.version)
	mux.HandleFunc("GET /v1/productos", s.productos)
	mux.HandleFunc("GET /v1/productos/{id}", s.producto)
	mux.HandleFunc("GET /v1/cambios", s.cambios)
	// /healthz is the only route without a key. It reports whether Redis answers and nothing else, and
	// the reverse proxy does not forward it (only /v1/ is exposed).
	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", s.healthz)
	root.Handle("/", s.auth(s.limit(mux)))
	return root, nil
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func lowerSet(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[strings.ToLower(strings.TrimSpace(x))] = true
	}
	return m
}

func plainName(s string) bool {
	for _, r := range s {
		if r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return s != ""
}

// --- authentication and plumbing ---------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func (s *server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		c, ok := s.identify(r.Header.Get("X-API-Key"))
		if !ok {
			s.log.Warn("request rejected", "path", r.URL.Path, "remote", r.RemoteAddr)
			writeErr(w, http.StatusUnauthorized, "a valid X-API-Key header is required")
			return
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), consumerKey{}, c)))
		s.log.Info("request", "consumer", c.Name, "method", r.Method, "path", r.URL.Path, "status", rec.status)
	})
}

// limit bounds the work a key holder can put on Redis: a fixed number of requests at a time (the
// rest get 503 and can retry) and a deadline on every request. Authentication comes first, so a
// caller without a valid key never takes a slot.
func (s *server) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusServiceUnavailable, "too many requests in progress: retry shortly")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// identify finds the consumer of a key. It compares against every consumer so the time does not
// reveal which one (or how much of a hash) matched.
func (s *server) identify(key string) (Consumer, bool) {
	if key == "" {
		return Consumer{}, false
	}
	sum := sha256.Sum256([]byte(key))
	got := []byte(hex.EncodeToString(sum[:]))
	var found Consumer
	ok := false
	for _, c := range s.cons {
		if subtle.ConstantTimeCompare(got, []byte(c.KeySHA256)) == 1 {
			found, ok = c, true
		}
	}
	return found, ok
}

func consumerOf(r *http.Request) Consumer {
	c, _ := r.Context().Value(consumerKey{}).(Consumer)
	return c
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

// fail answers a Redis or internal failure without leaking its text.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, publish.ErrNothingPublished):
		writeErr(w, http.StatusServiceUnavailable, "no data has been published yet")
	case errors.Is(err, publish.ErrVersionGone):
		writeErr(w, http.StatusGone, "that version is no longer available: start again without a cursor")
	default:
		s.log.Error("request failed", "path", r.URL.Path, "error", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

// intParam reads an integer query parameter within [min, max]; ok is false when it is malformed.
func intParam(q url.Values, name string, def, min, max int) (int, bool) {
	if !q.Has(name) {
		return def, true
	}
	n, err := strconv.Atoi(q.Get(name))
	if err != nil || n < min || n > max {
		return 0, false
	}
	return n, true
}

// --- price filtering ---------------------------------------------------------------------

// isPrice says whether a field name is one of the price fields. The comparison ignores case and
// surrounding spaces: the rows are stored exactly as the upstream API sent them, so "Precio_Venta"
// must be hidden as much as "precio_venta".
func (s *server) isPrice(name string) bool {
	return s.priceLC[strings.ToLower(strings.TrimSpace(name))]
}

// view shapes a row for a consumer: only the configured columns are kept (matched exactly, at the top
// level) and, for a consumer that may not see prices, the price fields are removed at any depth
// (objects inside objects and arrays). Numbers keep the text they were published with. When there is
// nothing to cut the row is passed through untouched.
func (s *server) view(c Consumer, raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 || (c.Precios && s.fields == nil) {
		return raw, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if m, ok := v.(map[string]any); ok && s.fields != nil {
		for k := range m {
			if !s.fields[k] {
				delete(m, k)
			}
		}
	}
	if !c.Precios {
		v = s.withoutPrices(v)
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(out.Bytes(), "\n"), nil
}

// returned says whether the API returns a column at all.
func (s *server) returned(field string) bool { return s.fields == nil || s.fields[field] }

// visible says whether a consumer receives a column: it must be one the API returns and, without
// price access, not a price.
func (s *server) visible(c Consumer, field string) bool {
	return s.returned(field) && (c.Precios || !s.isPrice(field))
}

func (s *server) withoutPrices(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if s.isPrice(k) {
				delete(t, k)
			} else {
				t[k] = s.withoutPrices(child)
			}
		}
	case []any:
		for i, child := range t {
			t[i] = s.withoutPrices(child)
		}
	}
	return v
}

func (s *server) views(c Consumer, rows []publish.Raw) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(rows))
	for _, r := range rows {
		v, err := s.view(c, r.JSON)
		if err != nil {
			return nil, fmt.Errorf("row %s: %w", r.ID, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// --- handlers ----------------------------------------------------------------------------

// healthz is for monitors on the same host: 200 when Redis answers, 503 otherwise. It takes no request
// slot, so it still answers when the API is busy, and it logs only failures.
func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	if err := s.src.Ping(ctx); err != nil {
		s.log.Warn("health check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) version(w http.ResponseWriter, r *http.Request) {
	m, err := s.src.Meta(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The cursor that goes with this version is recorded when the version is published. The tail of
	// the stream is not it: while a newer version is being written its events are already there, and
	// a consumer that loads this version and follows the tail would never see them.
	upTo := m.ChangesUpTo
	if upTo == "" { // published by a nt-sync without that field
		if upTo, err = s.src.ChangesEnd(r.Context()); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": m.Version, "generado_en": m.GeneradoEn, "publicado_en": m.PublicadoEn,
		"total": m.Total, "ultimo_cambio": upTo,
	})
}

func (s *server) producto(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ver, err := s.src.Current(ctx)
	if err != nil || ver == "" {
		if err == nil {
			err = publish.ErrNothingPublished
		}
		s.fail(w, r, err)
		return
	}
	raw, ok, err := s.src.Row(ctx, ver, r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "no product with that id")
		return
	}
	v, err := s.view(consumerOf(r), raw)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *server) productos(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := intParam(q, "limit", defaultLimit, 1, maxLimit)
	if !ok {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("limit must be an integer between 1 and %d", maxLimit))
		return
	}
	offset, ok := intParam(q, "offset", 0, 0, maxOffset)
	if !ok {
		writeErr(w, http.StatusBadRequest, "offset must be a non-negative integer")
		return
	}
	var filters []string
	for name := range q {
		switch name {
		case "cursor", "limit", "offset", "q":
		default:
			filters = append(filters, name)
		}
	}
	switch {
	case len(filters) > 1:
		writeErr(w, http.StatusBadRequest, "use one filter at a time")
	case len(filters) == 1 && q.Has("q"):
		writeErr(w, http.StatusBadRequest, "a field filter and a text search cannot be combined")
	case len(filters) == 1:
		s.byField(w, r, filters[0], q.Get(filters[0]), offset, limit)
	case q.Has("q"):
		s.byText(w, r, q.Get("q"), offset, limit)
	default:
		s.list(w, r, q.Get("cursor"), limit)
	}
}

func (s *server) list(w http.ResponseWriter, r *http.Request, token string, limit int) {
	ctx := r.Context()
	var ver string
	var cursor uint64
	var skip int
	if token != "" {
		var ok bool
		if ver, cursor, skip, ok = decodeCursor(token); !ok {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
	} else {
		var err error
		if ver, err = s.src.Current(ctx); err != nil || ver == "" {
			if err == nil {
				err = publish.ErrNothingPublished
			}
			s.fail(w, r, err)
			return
		}
	}
	rows, next, nextSkip, err := s.src.Page(ctx, ver, cursor, skip, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	prods, err := s.views(consumerOf(r), rows)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	siguiente := ""
	if next != 0 || nextSkip != 0 {
		siguiente = encodeCursor(ver, next, nextSkip)
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": ver, "productos": prods, "siguiente": siguiente})
}

func (s *server) byField(w http.ResponseWriter, r *http.Request, field, value string, offset, limit int) {
	c := consumerOf(r)
	// A filter is a question about the stored value, so a column the API does not return cannot be
	// used: the answers would let a caller find out what it holds.
	if !s.index[field] || !s.returned(field) {
		fields := make([]string, 0, len(s.index))
		for f := range s.index {
			if s.visible(c, f) {
				fields = append(fields, f)
			}
		}
		slices.Sort(fields)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "that field cannot be used as a filter", "campos": fields})
		return
	}
	if s.isPrice(field) && !c.Precios {
		// a filter by price would reveal it even if the column is hidden
		writeErr(w, http.StatusForbidden, "this API key cannot use price fields")
		return
	}
	ctx := r.Context()
	ver, err := s.src.Current(ctx)
	if err != nil || ver == "" {
		if err == nil {
			err = publish.ErrNothingPublished
		}
		s.fail(w, r, err)
		return
	}
	ids, err := s.src.Lookup(ctx, ver, field, value)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	total := len(ids)
	start := min(offset, total)
	end := min(start+limit, total)
	rows, err := s.src.RowsByID(ctx, ver, ids[start:end])
	if err != nil {
		s.fail(w, r, err)
		return
	}
	prods, err := s.views(c, rows)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	siguiente := ""
	if end < total {
		siguiente = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": ver, "total": total, "productos": prods, "siguiente": siguiente})
}

func (s *server) byText(w http.ResponseWriter, r *http.Request, text string, offset, limit int) {
	c := consumerOf(r)
	if len(s.search) == 0 {
		writeErr(w, http.StatusNotImplemented, "text search is not enabled on this server")
		return
	}
	var fields []string
	for _, f := range s.search {
		if s.visible(c, f) {
			fields = append(fields, f)
		}
	}
	if len(fields) == 0 {
		writeErr(w, http.StatusForbidden, "this API key cannot search the available fields")
		return
	}
	query, err := buildQuery(fields, text)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()
	ver, ids, total, err := s.src.SearchIDs(ctx, query, offset, limit)
	if err != nil {
		s.log.Error("search failed", "error", err)
		writeErr(w, http.StatusServiceUnavailable, "text search is not available right now")
		return
	}
	if ver == "" {
		if ver, err = s.src.Current(ctx); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	rows, err := s.src.RowsByID(ctx, ver, ids)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	prods, err := s.views(c, rows)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	siguiente := ""
	if end := offset + len(ids); end < total {
		siguiente = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": ver, "total": total, "productos": prods, "siguiente": siguiente})
}

// buildQuery turns free text into a RediSearch query. Only letters and digits of the terms reach
// the query (accents folded like the index), so the input cannot add operators, fields or wildcards.
func buildQuery(fields []string, text string) (string, error) {
	if len([]rune(text)) > maxQueryLen {
		return "", fmt.Errorf("q is limited to %d characters", maxQueryLen)
	}
	words := strings.FieldsFunc(publish.Fold(text), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	var terms []string
	for _, w := range words {
		if len(w) >= minTermLen && len(terms) < maxTerms {
			terms = append(terms, "*"+w+"*")
		}
	}
	if len(terms) == 0 {
		return "", fmt.Errorf("q needs at least one word of %d or more letters or digits", minTermLen)
	}
	group := "(" + strings.Join(terms, " ") + ")"
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, "@"+f+":"+group)
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, ")|(") + ")", nil
}

type eventView struct {
	Evento   string          `json:"evento"`
	Tipo     string          `json:"tipo"`
	ID       string          `json:"id"`
	Version  string          `json:"version"`
	Anterior string          `json:"anterior"`
	Campos   []string        `json:"campos,omitempty"`
	Antes    json.RawMessage `json:"antes,omitempty"`
	Fila     json.RawMessage `json:"fila"`
}

func (s *server) cambios(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	desde, valid := normalizeStreamID(q.Get("desde"))
	if !valid {
		writeErr(w, http.StatusBadRequest, "desde is required: the stream id of the last change you processed (\"ultimo_cambio\" of /v1/version, or \"siguiente\")")
		return
	}
	limit, ok := intParam(q, "limit", defaultLimit, 1, maxLimit)
	if !ok {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("limit must be an integer between 1 and %d", maxLimit))
		return
	}
	events, hasMore, lost, err := s.src.ChangesSince(r.Context(), desde, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if lost {
		writeJSON(w, http.StatusGone, map[string]any{
			"error":   "the changes after that point are no longer kept: reload every product and start again from /v1/version",
			"perdido": true,
		})
		return
	}
	c := consumerOf(r)
	out := make([]eventView, 0, len(events))
	siguiente := desde
	for _, e := range events {
		siguiente = e.StreamID // the cursor moves past events this consumer does not receive
		v, ok, err := s.eventFor(c, e)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if ok {
			out = append(out, v)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"eventos": out, "siguiente": siguiente, "hay_mas": hasMore})
}

// eventFor adapts a change to a consumer: rows carry only the columns the API returns, the price
// fields are removed for one without price access, and an update that touched nothing the consumer
// can see is not delivered at all.
func (s *server) eventFor(c Consumer, e publish.ChangeEvent) (eventView, bool, error) {
	v := eventView{Evento: e.StreamID, Tipo: e.Kind, ID: e.ID, Version: e.Version, Anterior: e.Previous, Campos: e.Fields, Antes: e.Before, Fila: e.Row}
	if c.Precios && s.fields == nil {
		return v, true, nil
	}
	var err error
	if v.Fila, err = s.view(c, e.Row); err != nil {
		return v, false, err
	}
	if v.Antes, err = s.view(c, e.Before); err != nil {
		return v, false, err
	}
	v.Campos = nil
	for _, f := range e.Fields {
		if s.visible(c, f) {
			v.Campos = append(v.Campos, f)
		}
	}
	return v, e.Kind != "modificado" || len(v.Campos) > 0, nil
}

// normalizeStreamID checks a stream id and rewrites it without leading zeros, so that "0001-02" and
// "1-2" are the same cursor (the boundary entry is dropped by comparing ids as text).
func normalizeStreamID(id string) (string, bool) {
	ms, seq, ok := strings.Cut(id, "-")
	if !ok || ms == "" || seq == "" {
		return "", false
	}
	m, e1 := strconv.ParseUint(ms, 10, 64)
	n, e2 := strconv.ParseUint(seq, 10, 64)
	if e1 != nil || e2 != nil {
		return "", false
	}
	return fmt.Sprintf("%d-%d", m, n), true
}

// --- pagination cursor -------------------------------------------------------------------

// The cursor carries the version it started on, so a long read keeps one consistent snapshot.
func encodeCursor(ver string, cursor uint64, skip int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(ver + "|" + strconv.FormatUint(cursor, 10) + "|" + strconv.Itoa(skip)))
}

func decodeCursor(token string) (ver string, cursor uint64, skip int, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", 0, 0, false
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 3 {
		return "", 0, 0, false
	}
	ver = parts[0]
	cursor, err1 := strconv.ParseUint(parts[1], 10, 64)
	skip, err2 := strconv.Atoi(parts[2])
	// the version becomes part of a Redis key: accept only what a version looks like
	if err1 != nil || err2 != nil || skip < 0 || ver == "" || len(ver) > 128 {
		return "", 0, 0, false
	}
	for _, r := range ver {
		if r != '.' && r != '_' && r != '-' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return "", 0, 0, false
		}
	}
	return ver, cursor, skip, true
}
