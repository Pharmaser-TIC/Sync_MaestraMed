// Package model holds the data shared by every layer: the snapshot published by the
// medication-creation API and the rows derived from it for Redis.
package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// VersionInfo is the answer of the nt_version action.
type VersionInfo struct {
	Version string         `json:"version"`
	Items   int            `json:"items"`
	Cargas  map[string]int `json:"cargas"`
}

// Snapshot is the answer of the nt_snapshot action. Rows are kept as raw JSON so that a
// new field in the API reaches the consumers without a change here.
type Snapshot struct {
	Version        string            `json:"version"`
	GeneradoEn     string            `json:"generado_en"`
	Total          int               `json:"total"`
	ConPrecios     bool              `json:"con_precios"`
	BasesFaltantes json.RawMessage   `json:"bases_faltantes"`
	Filas          []json.RawMessage `json:"filas"`
}

// Row is one snapshot row ready to be published.
type Row struct {
	Key   string            // unique inside the snapshot
	JSON  []byte            // the row exactly as the API sent it
	Index map[string]string // normalized value of every indexed field present in the row
}

// Rows derives the unique key and the index values of every row. The key is the row "id";
// when several rows share it (one line with several PLU) the next ones get "#2", "#3"...
// Index values are trimmed and upper-cased, like the matching rules of the API.
func (s Snapshot) Rows(indexFields []string) ([]Row, error) {
	rows := make([]Row, 0, len(s.Filas))
	seen := make(map[string]int, len(s.Filas))
	for i, raw := range s.Filas {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("row %d is not a JSON object: %w", i, err)
		}
		key := scalar(fields["id"])
		if key == "" {
			key = fmt.Sprintf("n%d", i+1)
		}
		seen[key]++
		if n := seen[key]; n > 1 {
			key = fmt.Sprintf("%s#%d", key, n)
		}
		index := make(map[string]string, len(indexFields))
		for _, f := range indexFields {
			if v := strings.ToUpper(strings.TrimSpace(scalar(fields[f]))); v != "" {
				index[f] = v
			}
		}
		rows = append(rows, Row{Key: key, JSON: raw, Index: index})
	}
	return rows, nil
}

// scalar returns the text of a JSON string or number; null, booleans, objects and arrays give "".
func scalar(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	switch raw[0] {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return ""
		}
		return s
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return string(raw)
	}
	return ""
}
