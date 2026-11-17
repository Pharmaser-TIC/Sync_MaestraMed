package publish

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"

	"nt-sync/internal/model"
)

// Change stream. When enabled, every publication compares the rows of the version readers use
// right now (<prefix>:actual) with the rows about to be published and appends one entry per
// row that was created, modified or deleted to the stream <prefix>:cambios:
//
//	tipo      creado | modificado | eliminado
//	id        row key (the row "id", "#2", "#3"... when a line has several PLU)
//	version   version that introduced the change
//	anterior  version it was compared with
//	campos    modificado only: changed fields, comma separated and sorted
//	antes     modificado only: JSON object with the previous value of each changed field (null when absent)
//	fila      the row as published (for eliminado, the row as it was)
//
// Entries are written before <prefix>:actual switches, so a consumer that sees a new version
// already finds its entries. If a publication fails after writing them and is retried, they are
// written again: delivery is at-least-once and consumers dedupe by (version, id).
// The first publication has nothing to compare with and emits nothing.

const (
	kindCreated  = "creado"
	kindModified = "modificado"
	kindDeleted  = "eliminado"
)

// WithChanges turns the change stream on, keeping about maxLen entries (older ones are
// trimmed). maxLen <= 0 keeps it off. It returns p to allow chaining.
func (p *Publisher) WithChanges(maxLen int64) *Publisher {
	p.changesMax = maxLen
	return p
}

// ChangesStream is the name of the stream consumers read.
func (p *Publisher) ChangesStream() string { return p.key("cambios") }

func (p *Publisher) changesOn() bool { return p.changesMax > 0 }

type change struct {
	kind, id, fila string
	campos, antes  string
}

// emitChanges appends the differences between version prev and rows (the content of ver).
func (p *Publisher) emitChanges(ctx context.Context, prev, ver string, rows []model.Row) error {
	if prev == "" || prev == ver {
		return nil
	}
	// Without the previous rows every row would look new: better no events than a flood.
	if n, err := p.rdb.Exists(ctx, p.key(prev, "filas")).Result(); err != nil || n == 0 {
		return err
	}
	changes, err := p.diff(ctx, prev, rows)
	if err != nil {
		return err
	}
	for start := 0; start < len(changes); start += batchSize {
		end := min(start+batchSize, len(changes))
		pipe := p.rdb.Pipeline()
		for _, c := range changes[start:end] {
			values := []any{"tipo", c.kind, "id", c.id, "version", ver, "anterior", prev, "fila", c.fila}
			if c.kind == kindModified {
				values = append(values, "campos", c.campos, "antes", c.antes)
			}
			pipe.XAdd(ctx, &redis.XAddArgs{Stream: p.ChangesStream(), MaxLen: p.changesMax, Approx: true, Values: values})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("writing changes %d-%d: %w", start, end, err)
		}
	}
	return nil
}

// diff compares the stored rows of prev with the new ones. Result order is deterministic:
// modified and deleted rows by key, then created rows in snapshot order.
func (p *Publisher) diff(ctx context.Context, prev string, rows []model.Row) ([]change, error) {
	next := make(map[string][]byte, len(rows))
	for _, r := range rows {
		next[r.Key] = r.JSON
	}
	var out []change
	seen := make(map[string]struct{}, len(rows))

	var cursor uint64
	for {
		kv, nextCursor, err := p.rdb.HScan(ctx, p.key(prev, "filas"), cursor, "*", 1000).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i+1 < len(kv); i += 2 {
			id, old := kv[i], []byte(kv[i+1])
			seen[id] = struct{}{}
			cur, ok := next[id]
			switch {
			case !ok:
				out = append(out, change{kind: kindDeleted, id: id, fila: string(old)})
			case !bytes.Equal(old, cur):
				fields, antes, err := fieldChanges(old, cur)
				if err != nil {
					return nil, fmt.Errorf("row %s: %w", id, err)
				}
				if len(fields) > 0 {
					out = append(out, change{kind: kindModified, id: id, fila: string(cur), campos: strings.Join(fields, ","), antes: antes})
				}
			}
		}
		if cursor = nextCursor; cursor == 0 {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })

	for _, r := range rows {
		if _, ok := seen[r.Key]; !ok {
			out = append(out, change{kind: kindCreated, id: r.Key, fila: string(r.JSON)})
		}
	}
	return out, nil
}

// fieldChanges lists the fields whose value differs between two row objects, and a JSON object
// with their previous values.
func fieldChanges(old, cur []byte) (fields []string, antes string, err error) {
	var a, b map[string]json.RawMessage
	if err := json.Unmarshal(old, &a); err != nil {
		return nil, "", err
	}
	if err := json.Unmarshal(cur, &b); err != nil {
		return nil, "", err
	}
	previous := map[string]json.RawMessage{}
	for f, va := range a {
		if vb, ok := b[f]; !ok || !bytes.Equal(va, vb) {
			fields = append(fields, f)
			previous[f] = va
		}
	}
	for f := range b {
		if _, ok := a[f]; !ok {
			fields = append(fields, f)
			previous[f] = json.RawMessage("null")
		}
	}
	sort.Strings(fields)
	out, err := json.Marshal(previous)
	return fields, string(out), err
}
