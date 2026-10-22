// Package store keeps the last downloaded snapshot in a local SQLite file, so Redis can be
// rebuilt (or the service restarted) without asking the API to calculate again.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // pure Go driver: no CGO needed

	"nt-sync/internal/model"
)

// Store is the SQLite-backed snapshot store.
type Store struct{ db *sql.DB }

// Open creates the file (and its folder) when missing and prepares the schema.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS meta (version TEXT NOT NULL, generado_en TEXT, total INTEGER, con_precios INTEGER, bases_faltantes TEXT)`,
		`CREATE TABLE IF NOT EXISTS filas (seq INTEGER PRIMARY KEY, json BLOB NOT NULL)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			db.Close()
			return nil, fmt.Errorf("preparing %s: %w", path, err)
		}
	}
	return &Store{db: db}, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// Version returns the stored snapshot version, if there is one.
func (s *Store) Version(ctx context.Context) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT version FROM meta LIMIT 1`).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return v, err == nil, err
}

// Save replaces the stored snapshot in a single transaction.
func (s *Store) Save(ctx context.Context, snap model.Snapshot) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if _, err := tx.ExecContext(ctx, `DELETE FROM meta`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM filas`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta (version, generado_en, total, con_precios, bases_faltantes) VALUES (?, ?, ?, ?, ?)`,
		snap.Version, snap.GeneradoEn, snap.Total, snap.ConPrecios, string(snap.BasesFaltantes)); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO filas (seq, json) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, f := range snap.Filas {
		if _, err := stmt.ExecContext(ctx, i, []byte(f)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Load returns the stored snapshot, if there is one.
func (s *Store) Load(ctx context.Context) (model.Snapshot, bool, error) {
	var snap model.Snapshot
	var bases sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT version, generado_en, total, con_precios, bases_faltantes FROM meta LIMIT 1`).
		Scan(&snap.Version, &snap.GeneradoEn, &snap.Total, &snap.ConPrecios, &bases)
	if err == sql.ErrNoRows {
		return snap, false, nil
	}
	if err != nil {
		return snap, false, err
	}
	snap.BasesFaltantes = json.RawMessage("[]")
	if bases.Valid && bases.String != "" {
		snap.BasesFaltantes = json.RawMessage(bases.String)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT json FROM filas ORDER BY seq`)
	if err != nil {
		return snap, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return snap, false, err
		}
		snap.Filas = append(snap.Filas, json.RawMessage(b))
	}
	return snap, true, rows.Err()
}
