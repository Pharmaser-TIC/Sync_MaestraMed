package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Read side of the layout described in redis.go, changes.go and search.go. Everything here only
// reads: it is what the HTTP API uses, and it works with a Redis user that has read commands only.

var (
	// ErrVersionGone means the version a reader was using has been pruned.
	ErrVersionGone = errors.New("version is no longer available")
	// ErrNothingPublished means <prefix>:actual is empty.
	ErrNothingPublished = errors.New("nothing has been published")
)

// Raw is one stored row: its key and the JSON exactly as it was published.
type Raw struct {
	ID   string
	JSON json.RawMessage
}

// Meta is the metadata of the current version.
type Meta struct {
	Version     string
	GeneradoEn  string
	PublicadoEn string
	Total       int64
	// ChangesUpTo is the change-stream id that goes with this version: the last entry written
	// before it became current. "" when the version was published without that information.
	ChangesUpTo string
}

// ChangeEvent is one entry of the change stream (see changes.go).
type ChangeEvent struct {
	StreamID string
	Kind     string // creado | modificado | eliminado
	ID       string // row key
	Version  string
	Previous string
	Fields   []string        // modificado only
	Before   json.RawMessage // modificado only: previous value of each changed field
	Row      json.RawMessage
}

// Ping checks that Redis answers.
func (p *Publisher) Ping(ctx context.Context) error { return p.rdb.Ping(ctx).Err() }

// Meta returns the metadata of the version readers use now.
func (p *Publisher) Meta(ctx context.Context) (Meta, error) {
	ver, err := p.Current(ctx)
	if err != nil {
		return Meta{}, err
	}
	if ver == "" {
		return Meta{}, ErrNothingPublished
	}
	m, _, err := p.metaOf(ctx, ver)
	return m, err
}

// MetaOf returns the metadata of a published version, the current one or an older one that is still
// kept. ErrVersionGone means it is not listed in <prefix>:versiones (never published, or pruned).
func (p *Publisher) MetaOf(ctx context.Context, ver string) (Meta, error) {
	if ok, err := p.hasVersion(ctx, ver); err != nil {
		return Meta{}, err
	} else if !ok {
		return Meta{}, ErrVersionGone
	}
	m, found, err := p.metaOf(ctx, ver)
	if err == nil && !found {
		err = ErrVersionGone
	}
	return m, err
}

// metaOf reads the meta hash of ver; found is false when the hash does not exist.
func (p *Publisher) metaOf(ctx context.Context, ver string) (m Meta, found bool, err error) {
	h, err := p.rdb.HGetAll(ctx, p.key(ver, "meta")).Result()
	if err != nil {
		return Meta{}, false, err
	}
	m = Meta{Version: ver, GeneradoEn: h["generado_en"], PublicadoEn: h["publicado_en"], ChangesUpTo: h["cambios_hasta"]}
	m.Total, _ = strconv.ParseInt(h["total"], 10, 64)
	return m, len(h) > 0, nil
}

// Versions returns the versions kept in Redis, oldest first: the order of <prefix>:versiones, which is
// scored by publication time. Only complete versions are listed (see Page).
func (p *Publisher) Versions(ctx context.Context) ([]string, error) {
	return p.rdb.ZRange(ctx, p.key("versiones"), 0, -1).Result()
}

func (p *Publisher) hasVersion(ctx context.Context, ver string) (bool, error) {
	_, err := p.rdb.ZScore(ctx, p.key("versiones"), ver).Result()
	if err == redis.Nil {
		return false, nil
	}
	return err == nil, err
}

// Page returns at most `limit` rows of version ver, starting at position (cursor, skip), and the
// position to continue from; (0, 0) means there is nothing more. HSCAN's COUNT is only a hint (a
// small hash comes back whole), so a batch can be longer than limit: `skip` says how many rows of
// the batch at `cursor` were already delivered. A published version never changes, so scanning
// from the same cursor returns the same batch.
func (p *Publisher) Page(ctx context.Context, ver string, cursor uint64, skip, limit int) ([]Raw, uint64, int, error) {
	key := p.key(ver, "filas")
	// Only versions listed in <prefix>:versiones are complete: one that is still being written (or
	// was never published) is not readable even if someone names it.
	if _, err := p.rdb.ZScore(ctx, p.key("versiones"), ver).Result(); err == redis.Nil {
		return nil, 0, 0, ErrVersionGone
	} else if err != nil {
		return nil, 0, 0, err
	}
	if n, err := p.rdb.Exists(ctx, key).Result(); err != nil {
		return nil, 0, 0, err
	} else if n == 0 {
		return nil, 0, 0, ErrVersionGone
	}
	var out []Raw
	for {
		kv, next, err := p.rdb.HScan(ctx, key, cursor, "*", int64(limit)).Result()
		if err != nil {
			return nil, 0, 0, err
		}
		batch := make([]Raw, 0, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			batch = append(batch, Raw{ID: kv[i], JSON: json.RawMessage(kv[i+1])})
		}
		batch = batch[min(skip, len(batch)):]
		take := min(len(batch), limit-len(out))
		out = append(out, batch[:take]...)
		if take < len(batch) {
			return out, cursor, skip + take, nil // the batch has rows left
		}
		skip, cursor = 0, next
		if cursor == 0 || len(out) >= limit {
			return out, cursor, 0, nil
		}
	}
}

// Row returns one row of version ver.
func (p *Publisher) Row(ctx context.Context, ver, id string) ([]byte, bool, error) {
	v, err := p.rdb.HGet(ctx, p.key(ver, "filas"), id).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	return []byte(v), err == nil, err
}

// Lookup returns the sorted row keys whose indexed field equals value (trimmed, upper-case).
func (p *Publisher) Lookup(ctx context.Context, ver, field, value string) ([]string, error) {
	ids, err := p.rdb.SMembers(ctx, p.key(ver, "idx", field, strings.ToUpper(strings.TrimSpace(value)))).Result()
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// RowsByID returns the rows of version ver with the given keys, in that order; missing ones are skipped.
func (p *Publisher) RowsByID(ctx context.Context, ver string, ids []string) ([]Raw, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	vals, err := p.rdb.HMGet(ctx, p.key(ver, "filas"), ids...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Raw, 0, len(ids))
	for i, v := range vals {
		if s, ok := v.(string); ok {
			out = append(out, Raw{ID: ids[i], JSON: json.RawMessage(s)})
		}
	}
	return out, nil
}

// SearchIDs runs a RediSearch query through the alias of the current version and returns that
// version, the matching row keys and the total number of matches. The query is passed as is: the
// caller must build it from sanitized terms.
func (p *Publisher) SearchIDs(ctx context.Context, query string, offset, limit int) (string, []string, int, error) {
	return p.ftSearch(ctx, p.AliasName(), query, offset, limit)
}

// SearchIDsIn runs the same query against the index of a given version instead of the alias, and
// returns the matching row keys and the total. The version must be one that is still published.
func (p *Publisher) SearchIDsIn(ctx context.Context, ver, query string, offset, limit int) ([]string, int, error) {
	if ok, err := p.hasVersion(ctx, ver); err != nil {
		return nil, 0, err
	} else if !ok {
		return nil, 0, ErrVersionGone
	}
	_, ids, total, err := p.ftSearch(ctx, p.indexName(ver), query, offset, limit)
	return ids, total, err
}

func (p *Publisher) ftSearch(ctx context.Context, index, query string, offset, limit int) (string, []string, int, error) {
	res, err := p.rdb.FTSearchWithArgs(ctx, index, query, &redis.FTSearchOptions{
		NoContent: true, LimitOffset: offset, Limit: limit,
	}).Result()
	if err != nil {
		return "", nil, 0, err
	}
	var ver string
	ids := make([]string, 0, len(res.Docs))
	for _, d := range res.Docs {
		// document key: <prefix>:<version>:doc:<row key>
		v, id, ok := strings.Cut(strings.TrimPrefix(d.ID, p.prefix+":"), ":doc:")
		if !ok {
			return "", nil, 0, fmt.Errorf("unexpected document key %q", d.ID)
		}
		ver = v
		ids = append(ids, id)
	}
	return ver, ids, res.Total, nil
}

// ChangesEnd returns the id of the newest change-stream entry, "0-0" when the stream is empty.
func (p *Publisher) ChangesEnd(ctx context.Context) (string, error) {
	last, err := p.rdb.XRevRangeN(ctx, p.ChangesStream(), "+", "-", 1).Result()
	if err != nil || len(last) == 0 {
		return "0-0", err
	}
	return last[0].ID, nil
}

// ChangesSince returns up to limit events with an id greater than `after`, oldest first.
// hasMore says there are more after them. lost says entries between `after` and the start of the
// stream were trimmed, so the reader cannot follow the stream any more and must reload everything;
// A stream that was never trimmed loses nothing, whatever the cursor ("0-0" included).
func (p *Publisher) ChangesSince(ctx context.Context, after string, limit int) (events []ChangeEvent, hasMore, lost bool, err error) {
	// The stream was trimmed when more entries were ever added than it holds now. Then everything
	// older than its first entry is gone, so a cursor older than that missed some of it.
	// The three reads are one transaction: a publication trimming in between could otherwise
	// make them disagree.
	tx := p.rdb.TxPipeline()
	addedCmd := tx.Get(ctx, p.changesTotalKey())
	lengthCmd := tx.XLen(ctx, p.ChangesStream())
	firstCmd := tx.XRangeN(ctx, p.ChangesStream(), "-", "+", 1)
	if _, err := tx.Exec(ctx); err != nil && err != redis.Nil { // redis.Nil: no counter yet
		return nil, false, false, err
	}
	added, err := addedCmd.Int64()
	if err != nil && err != redis.Nil {
		return nil, false, false, err
	}
	if first := firstCmd.Val(); added > lengthCmd.Val() {
		if len(first) == 0 || streamIDLess(after, first[0].ID) {
			return nil, false, true, nil
		}
	}
	// The range is inclusive: ask for one more and drop the entry equal to `after`.
	msgs, err := p.rdb.XRangeN(ctx, p.ChangesStream(), after, "+", int64(limit)+2).Result()
	if err != nil {
		return nil, false, false, err
	}
	if len(msgs) > 0 && msgs[0].ID == after {
		msgs = msgs[1:]
	}
	if len(msgs) > limit {
		msgs, hasMore = msgs[:limit], true
	}
	for _, m := range msgs {
		events = append(events, toEvent(m))
	}
	return events, hasMore, false, nil
}

func toEvent(m redis.XMessage) ChangeEvent {
	str := func(k string) string { s, _ := m.Values[k].(string); return s }
	e := ChangeEvent{
		StreamID: m.ID, Kind: str("tipo"), ID: str("id"), Version: str("version"), Previous: str("anterior"),
		Row: json.RawMessage(str("fila")),
	}
	if c := str("campos"); c != "" {
		e.Fields = strings.Split(c, ",")
	}
	if b := str("antes"); b != "" {
		e.Before = json.RawMessage(b)
	}
	return e
}

// streamIDLess compares two stream ids of the form <ms>-<seq>.
func streamIDLess(a, b string) bool {
	am, as := splitStreamID(a)
	bm, bs := splitStreamID(b)
	if am != bm {
		return am < bm
	}
	return as < bs
}

func splitStreamID(id string) (ms, seq uint64) {
	m, s, _ := strings.Cut(id, "-")
	ms, _ = strconv.ParseUint(m, 10, 64)
	seq, _ = strconv.ParseUint(s, 10, 64)
	return
}
