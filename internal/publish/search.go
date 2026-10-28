package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Full-text search (RediSearch) is optional. When search fields are configured, every row is
// also stored as a JSON document and indexed per version:
//
//	<prefix>:<version>:doc:<row key>   JSON    the row plus "_busqueda" (folded search text), indexed by <prefix>:<version>:ft
//	<prefix>:<version>:ft              index   RediSearch index over the configured fields
//	<prefix>:ft                        alias   points to the index of the current version
//
// Readers query the alias, so it switches together with <prefix>:actual.

// foldAccents maps the accented letters of Spanish text to their plain form. RediSearch lower-cases
// terms but does not strip accents, so "acido*" would not find "ÁCIDO".
var foldAccents = strings.NewReplacer(
	"á", "a", "à", "a", "ä", "a", "â", "a", "ã", "a",
	"é", "e", "è", "e", "ë", "e", "ê", "e",
	"í", "i", "ì", "i", "ï", "i", "î", "i",
	"ó", "o", "ò", "o", "ö", "o", "ô", "o", "õ", "o",
	"ú", "u", "ù", "u", "ü", "u", "û", "u",
	"ñ", "n", "ç", "c",
)

// Fold lower-cases s and removes its accents. Search terms must go through it before they are
// sent to FT.SEARCH, because the index holds the text folded the same way.
func Fold(s string) string { return foldAccents.Replace(strings.ToLower(s)) }

// searchDoc returns the row as the JSON document to index: the row exactly as it came, plus a
// "_busqueda" object with the folded text of every search field. The indexed attributes read from it.
func (p *Publisher) searchDoc(raw []byte) (string, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	folded := make(map[string]string, len(p.search))
	for _, f := range p.search {
		var s string
		if json.Unmarshal(doc[f], &s) == nil && s != "" {
			folded[f] = Fold(s)
		}
	}
	b, err := json.Marshal(folded)
	if err != nil {
		return "", err
	}
	doc["_busqueda"] = b
	out, err := json.Marshal(doc)
	return string(out), err
}

// WithSearch enables full-text search over the given snapshot fields. It returns p to allow chaining.
func (p *Publisher) WithSearch(fields []string) *Publisher {
	p.search = fields
	return p
}

func (p *Publisher) searchOn() bool { return len(p.search) > 0 }

func (p *Publisher) indexName(ver string) string { return p.key(ver, "ft") }

// AliasName is the name readers use in FT.SEARCH; it always points to the current version.
func (p *Publisher) AliasName() string { return p.key("ft") }

// CheckSearch fails when the Redis server cannot run RediSearch commands.
func (p *Publisher) CheckSearch(ctx context.Context) error {
	if err := p.rdb.Do(ctx, "FT._LIST").Err(); err != nil {
		return fmt.Errorf("the search module is not available in this Redis (needed by NT_SEARCH_FIELDS): %w", err)
	}
	return nil
}

// createIndex creates the index before the documents are written, so each document is indexed
// as it arrives and the index is complete once the last one is stored.
func (p *Publisher) createIndex(ctx context.Context, ver string) error {
	args := []any{"FT.CREATE", p.indexName(ver), "ON", "JSON", "PREFIX", 1, p.key(ver, "doc") + ":", "STOPWORDS", 0, "SCHEMA"}
	for _, f := range p.search {
		args = append(args, "$._busqueda."+f, "AS", f, "TEXT", "NOSTEM", "WITHSUFFIXTRIE")
	}
	if err := p.rdb.Do(ctx, args...).Err(); err != nil {
		return fmt.Errorf("creating search index: %w", err)
	}
	return nil
}

// dropIndex removes the index of a version (its documents are removed with the other keys).
// Dropping an index that does not exist is not an error. It also works when search is
// disabled, so a version published with search is cleaned up after the setting is turned off.
func (p *Publisher) dropIndex(ctx context.Context, ver string) error {
	err := p.rdb.Do(ctx, "FT.DROPINDEX", p.indexName(ver)).Err()
	if err == nil || isNoSearch(err) {
		return nil
	}
	return fmt.Errorf("dropping search index: %w", err)
}

// pointAlias makes readers' alias use the index of ver.
func (p *Publisher) pointAlias(ctx context.Context, ver string) error {
	if err := p.rdb.Do(ctx, "FT.ALIASUPDATE", p.AliasName(), p.indexName(ver)).Err(); err != nil {
		return fmt.Errorf("switching search alias: %w", err)
	}
	return nil
}

// isNoSearch tells "this index does not exist" and "this server has no search module" apart
// from real failures.
func isNoSearch(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"unknown index", "no such index", "index not found", "unknown command"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
