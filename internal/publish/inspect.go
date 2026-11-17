package publish

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

const sampleMaxLen = 300

// VersionInfo describes one published version listed in <prefix>:versiones.
type VersionInfo struct {
	Version     string
	PublishedAt time.Time
	Rows        int64 // entries in <prefix>:<version>:filas
}

// Report is what Inspect found in Redis. It only reads; it never writes or deletes.
type Report struct {
	Prefix     string
	Current    string            // value of <prefix>:actual, "" when nothing is published
	Meta       map[string]string // <prefix>:<current>:meta
	Rows       int64             // entries in <prefix>:<current>:filas
	Indexes    map[string]int    // indexed field -> number of lookup sets
	SearchOn   bool              // full-text search is configured
	SearchDocs int64             // documents in the search index of the current version
	SampleKey  string
	SampleJSON string
	ChangesOn  bool          // the change stream is configured
	ChangesLen int64         // entries in <prefix>:cambios
	ChangesEnd string        // id of the newest entry, "" when empty
	Versions   []VersionInfo // newest first
	LockHeld   bool
	LockTTL    time.Duration
	Problems   []string
}

// OK reports whether the published data looks complete and consistent.
func (r Report) OK() bool { return len(r.Problems) == 0 }

// Inspect reads the published state back from Redis so an operator can check that a
// cycle really stored its data.
func (p *Publisher) Inspect(ctx context.Context) (Report, error) {
	r := Report{Prefix: p.prefix, Indexes: map[string]int{}}

	var err error
	if r.Current, err = p.Current(ctx); err != nil {
		return r, err
	}

	zs, err := p.rdb.ZRevRangeWithScores(ctx, p.key("versiones"), 0, -1).Result()
	if err != nil {
		return r, err
	}
	for _, z := range zs {
		ver, _ := z.Member.(string)
		n, err := p.rdb.HLen(ctx, p.key(ver, "filas")).Result()
		if err != nil {
			return r, err
		}
		r.Versions = append(r.Versions, VersionInfo{Version: ver, PublishedAt: time.Unix(0, int64(z.Score)).UTC(), Rows: n})
	}

	if n, err := p.rdb.Exists(ctx, p.key("lock")).Result(); err != nil {
		return r, err
	} else if n > 0 {
		r.LockHeld = true
		if ttl, err := p.rdb.PTTL(ctx, p.key("lock")).Result(); err == nil && ttl > 0 {
			r.LockTTL = ttl
		}
	}

	if p.changesOn() {
		r.ChangesOn = true
		if r.ChangesLen, err = p.rdb.XLen(ctx, p.ChangesStream()).Result(); err != nil {
			return r, err
		}
		if last, err := p.rdb.XRevRangeN(ctx, p.ChangesStream(), "+", "-", 1).Result(); err != nil {
			return r, err
		} else if len(last) == 1 {
			r.ChangesEnd = last[0].ID
		}
	}

	if r.Current == "" {
		r.Problems = append(r.Problems, p.key("actual")+" is empty: nothing has been published yet")
		return r, nil
	}
	ver := r.Current

	if r.Meta, err = p.rdb.HGetAll(ctx, p.key(ver, "meta")).Result(); err != nil {
		return r, err
	}
	if r.Rows, err = p.rdb.HLen(ctx, p.key(ver, "filas")).Result(); err != nil {
		return r, err
	}
	switch {
	case len(r.Meta) == 0:
		r.Problems = append(r.Problems, fmt.Sprintf("%s is missing: version %s has no metadata", p.key(ver, "meta"), ver))
	case r.Rows == 0:
		r.Problems = append(r.Problems, fmt.Sprintf("%s is missing or empty: version %s has no rows", p.key(ver, "filas"), ver))
	default:
		if total, err := strconv.ParseInt(r.Meta["total"], 10, 64); err != nil || total != r.Rows {
			r.Problems = append(r.Problems, fmt.Sprintf("meta says total=%s but %s holds %d rows", r.Meta["total"], p.key(ver, "filas"), r.Rows))
		}
	}

	if err := p.countIndexes(ctx, ver, r.Indexes); err != nil {
		return r, err
	}
	if p.searchOn() {
		r.SearchOn = true
		info, err := p.rdb.FTInfo(ctx, p.indexName(ver)).Result()
		switch {
		case err != nil && isNoSearch(err):
			r.Problems = append(r.Problems, fmt.Sprintf("search is configured but index %s does not exist", p.indexName(ver)))
		case err != nil:
			return r, err
		default:
			r.SearchDocs = int64(info.NumDocs)
			if r.SearchDocs != r.Rows {
				r.Problems = append(r.Problems, fmt.Sprintf("search index %s holds %d documents but there are %d rows", p.indexName(ver), r.SearchDocs, r.Rows))
			}
		}
	}
	if r.SampleKey, r.SampleJSON, err = p.sampleRow(ctx, ver); err != nil {
		return r, err
	}
	return r, nil
}

// countIndexes counts the lookup sets of a version per indexed field.
func (p *Publisher) countIndexes(ctx context.Context, ver string, into map[string]int) error {
	prefix := p.key(ver, "idx") + ":"
	var cursor uint64
	for {
		keys, next, err := p.rdb.Scan(ctx, cursor, prefix+"*", 1000).Result()
		if err != nil {
			return err
		}
		for _, k := range keys {
			field, _, _ := strings.Cut(strings.TrimPrefix(k, prefix), ":")
			into[field]++
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}

// sampleRow returns one stored row, or empty strings when the version has none.
func (p *Publisher) sampleRow(ctx context.Context, ver string) (key, js string, err error) {
	var cursor uint64
	for {
		kv, next, err := p.rdb.HScan(ctx, p.key(ver, "filas"), cursor, "*", 10).Result()
		if err != nil {
			return "", "", err
		}
		if len(kv) >= 2 {
			return kv[0], kv[1], nil
		}
		if cursor = next; cursor == 0 {
			return "", "", nil
		}
	}
}

// Write prints the report in a form meant to be read in a terminal.
func (r Report) Write(w io.Writer) {
	cur := r.Current
	if cur == "" {
		cur = "(empty)"
	}
	fmt.Fprintf(w, "%s:actual = %s\n", r.Prefix, cur)

	if r.Current != "" {
		fmt.Fprintln(w, "\nCurrent version")
		for _, k := range []string{"generado_en", "publicado_en", "total", "con_precios", "bases_faltantes"} {
			v, ok := r.Meta[k]
			if !ok {
				v = "(missing)"
			}
			fmt.Fprintf(w, "  meta.%-16s %s\n", k, v)
		}
		fmt.Fprintf(w, "  %-21s %d\n", "rows stored (filas)", r.Rows)
		fields := make([]string, 0, len(r.Indexes))
		for f := range r.Indexes {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		if len(fields) == 0 {
			fmt.Fprintf(w, "  %-21s none\n", "lookup sets")
		}
		for _, f := range fields {
			fmt.Fprintf(w, "  %-21s %d\n", "idx."+f, r.Indexes[f])
		}
		if r.SearchOn {
			fmt.Fprintf(w, "  %-21s %d\n", "search documents", r.SearchDocs)
		}
		if r.SampleKey != "" {
			js := r.SampleJSON
			if len(js) > sampleMaxLen {
				js = js[:sampleMaxLen] + "..."
			}
			fmt.Fprintf(w, "  sample row %s: %s\n", r.SampleKey, js)
		}
	}

	if r.ChangesOn {
		last := r.ChangesEnd
		if last == "" {
			last = "(none)"
		}
		fmt.Fprintf(w, "\nChange stream %s: %d entries, newest %s\n", r.Prefix+":cambios", r.ChangesLen, last)
	}

	if len(r.Versions) > 0 {
		fmt.Fprintln(w, "\nVersions in Redis (newest first)")
		for _, v := range r.Versions {
			mark := " "
			if v.Version == r.Current {
				mark = "*"
			}
			fmt.Fprintf(w, "  %s %s  published %s  rows %d\n", mark, v.Version, v.PublishedAt.Format(time.RFC3339), v.Rows)
		}
	}

	if r.LockHeld {
		fmt.Fprintf(w, "\nLock: held (expires in %s)\n", r.LockTTL.Round(time.Second))
	} else {
		fmt.Fprintln(w, "\nLock: free")
	}

	if r.OK() {
		fmt.Fprintln(w, "\nResult: OK")
		return
	}
	fmt.Fprintln(w, "\nResult: PROBLEMS")
	for _, p := range r.Problems {
		fmt.Fprintf(w, "  - %s\n", p)
	}
}
