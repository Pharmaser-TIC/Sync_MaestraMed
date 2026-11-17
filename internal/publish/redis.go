// Package publish writes snapshots to Redis under versioned keys and switches a single
// pointer key once a version is complete, so readers never see a half-written snapshot.
//
// Layout (prefix defaults to "nt"):
//
//	<prefix>:actual                    string  version readers must use
//	<prefix>:<version>:meta            hash    version, generado_en, total, con_precios, bases_faltantes, publicado_en
//	<prefix>:<version>:filas           hash    row key -> row JSON
//	<prefix>:<version>:idx:<field>:<V> set     row keys whose <field> equals V (trimmed, upper-case)
//	<prefix>:versiones                 zset    published versions, scored by publication time
//
// With search fields configured (see search.go) a version also holds one JSON document per row,
// <prefix>:<version>:doc:<row key>, a RediSearch index <prefix>:<version>:ft and the alias
// <prefix>:ft that points to the index of the current version.
package publish

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"nt-sync/internal/model"
)

const batchSize = 500

const unlockScript = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end return 0`

// Publisher publishes snapshots to Redis.
type Publisher struct {
	rdb    redis.UniversalClient
	prefix string
	keep   int
	search []string // fields indexed for full-text search; empty means search is off

	changesMax int64 // approximate length of the change stream; 0 means it is off (see changes.go)
}

// New returns a Publisher that keeps the newest `keep` published versions (at least 1).
func New(rdb redis.UniversalClient, prefix string, keep int) *Publisher {
	if keep < 1 {
		keep = 1
	}
	return &Publisher{rdb: rdb, prefix: prefix, keep: keep}
}

func (p *Publisher) key(parts ...string) string { return p.prefix + ":" + strings.Join(parts, ":") }

// Current returns the version readers are using, or "" when nothing is published.
func (p *Publisher) Current(ctx context.Context) (string, error) {
	v, err := p.rdb.Get(ctx, p.key("actual")).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// Publish writes the whole version and, only when it is complete, points <prefix>:actual to it.
// Older versions beyond the retention limit are removed afterwards.
func (p *Publisher) Publish(ctx context.Context, snap model.Snapshot, rows []model.Row) error {
	ver := snap.Version
	if ver == "" {
		return fmt.Errorf("snapshot without version")
	}
	// The version readers use now is the baseline of the change stream.
	prev := ""
	if p.changesOn() {
		var err error
		if prev, err = p.Current(ctx); err != nil {
			return err
		}
	}
	// A previous attempt may have left a partial copy of this same version.
	if err := p.deleteVersion(ctx, ver); err != nil {
		return err
	}

	if p.searchOn() {
		if err := p.createIndex(ctx, ver); err != nil {
			return err
		}
	}

	filasKey := p.key(ver, "filas")
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		pipe := p.rdb.Pipeline()
		fields := make([]any, 0, 2*(end-start))
		for _, r := range rows[start:end] {
			fields = append(fields, r.Key, r.JSON)
			for f, v := range r.Index {
				pipe.SAdd(ctx, p.key(ver, "idx", f, v), r.Key)
			}
			if p.searchOn() {
				doc, err := p.searchDoc(r.JSON)
				if err != nil {
					return fmt.Errorf("row %s: %w", r.Key, err)
				}
				pipe.JSONSet(ctx, p.key(ver, "doc", r.Key), "$", doc)
			}
		}
		pipe.HSet(ctx, filasKey, fields...)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("writing rows %d-%d: %w", start, end, err)
		}
	}

	conPrecios := "0"
	if snap.ConPrecios {
		conPrecios = "1"
	}
	now := time.Now()
	if err := p.rdb.HSet(ctx, p.key(ver, "meta"),
		"version", ver,
		"generado_en", snap.GeneradoEn,
		"total", len(rows),
		"con_precios", conPrecios,
		"bases_faltantes", string(snap.BasesFaltantes),
		"publicado_en", now.UTC().Format(time.RFC3339),
	).Err(); err != nil {
		return fmt.Errorf("writing meta: %w", err)
	}
	if err := p.rdb.ZAdd(ctx, p.key("versiones"), redis.Z{Score: float64(now.UnixNano()), Member: ver}).Err(); err != nil {
		return err
	}
	if p.changesOn() {
		if err := p.emitChanges(ctx, prev, ver, rows); err != nil {
			return fmt.Errorf("writing change stream: %w", err)
		}
		// Readers that load this version continue from here (see Meta.ChangesUpTo).
		end, err := p.ChangesEnd(ctx)
		if err != nil {
			return err
		}
		if err := p.rdb.HSet(ctx, p.key(ver, "meta"), "cambios_hasta", end).Err(); err != nil {
			return fmt.Errorf("writing change cursor: %w", err)
		}
	}
	if p.searchOn() {
		if err := p.pointAlias(ctx, ver); err != nil {
			return err
		}
	}
	// The switch: from here on readers use the new version.
	if err := p.rdb.Set(ctx, p.key("actual"), ver, 0).Err(); err != nil {
		return fmt.Errorf("switching pointer: %w", err)
	}
	return p.prune(ctx, ver)
}

// prune removes the versions older than the newest `keep`, never the one just published.
func (p *Publisher) prune(ctx context.Context, current string) error {
	versions, err := p.rdb.ZRevRange(ctx, p.key("versiones"), 0, -1).Result()
	if err != nil {
		return err
	}
	for i, v := range versions {
		if i < p.keep || v == current {
			continue
		}
		if err := p.deleteVersion(ctx, v); err != nil {
			return err
		}
		if err := p.rdb.ZRem(ctx, p.key("versiones"), v).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) deleteVersion(ctx context.Context, ver string) error {
	// The index goes first: deleting the documents of a live index would update it one by one.
	if err := p.dropIndex(ctx, ver); err != nil {
		return err
	}
	var cursor uint64
	for {
		keys, next, err := p.rdb.Scan(ctx, cursor, p.key(ver, "*"), 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := p.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}

// Lock lets a single instance synchronize at a time. ok is false when another instance holds it.
// The lock expires after ttl, so a crashed instance does not block the others forever.
func (p *Publisher) Lock(ctx context.Context, ttl time.Duration) (release func(), ok bool, err error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, false, err
	}
	token := hex.EncodeToString(buf)
	lockKey := p.key("lock")
	ok, err = p.rdb.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil || !ok {
		return nil, false, err
	}
	return func() {
		// Own context: the caller's may already be cancelled during shutdown.
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.rdb.Eval(c, unlockScript, []string{lockKey}, token)
	}, true, nil
}
