// Package httpapi serves the published products over HTTP to the projects that cannot reach Redis.
// It only reads, and it decides per consumer (API key) whether the price fields are returned.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Consumer is a project allowed to call the API.
type Consumer struct {
	Name      string // shown in the access log
	KeySHA256 string // lower-case hex SHA-256 of its API key; the key itself is never stored
	Precios   bool   // may see the fields listed in NT_PRICE_FIELDS
}

// consumersFile is the JSON the operator maintains, for example:
//
//	{"consumers":[{"name":"facturacion","key_sha256":"<64 hex>","precios":true}]}
//
// "precios" has no default on purpose: every project must be classified explicitly.
type consumersFile struct {
	Consumers []struct {
		Name      string `json:"name"`
		KeySHA256 string `json:"key_sha256"`
		Precios   *bool  `json:"precios"`
	} `json:"consumers"`
}

// LoadConsumers reads and validates the consumers file. Unknown fields are errors, so a typo
// such as "precio" cannot silently change who sees the prices.
func LoadConsumers(path string) ([]Consumer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading consumers file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f consumersFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("consumers file %s: %w", path, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("consumers file %s: trailing data after the JSON object", path)
	}
	if len(f.Consumers) == 0 {
		return nil, errors.New("consumers file: define at least one consumer")
	}
	names, hashes := map[string]bool{}, map[string]bool{}
	out := make([]Consumer, 0, len(f.Consumers))
	for i, c := range f.Consumers {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			return nil, fmt.Errorf("consumers file: consumer #%d has no name", i+1)
		}
		if c.Precios == nil {
			return nil, fmt.Errorf("consumers file: consumer %q must say \"precios\": true or false", name)
		}
		hash := strings.ToLower(strings.TrimSpace(c.KeySHA256))
		if !isSHA256Hex(hash) {
			return nil, fmt.Errorf("consumers file: consumer %q: key_sha256 must be 64 hex characters (sha256 of the API key)", name)
		}
		if names[name] {
			return nil, fmt.Errorf("consumers file: duplicate name %q", name)
		}
		if hashes[hash] {
			return nil, fmt.Errorf("consumers file: duplicate key_sha256 (consumer %q)", name)
		}
		names[name], hashes[hash] = true, true
		out = append(out, Consumer{Name: name, KeySHA256: hash, Precios: *c.Precios})
	}
	return out, nil
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
