// Package config reads the service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting of the service.
type Config struct {
	APIURL       string        // full URL of api.php, e.g. https://host/api.php
	APIKey       string        // value for the X-API-Key header
	PollInterval time.Duration // time between nt_version checks
	Redis        Redis
	SQLitePath   string
	IndexFields  []string // snapshot fields that get a lookup set in Redis
	SearchFields []string // snapshot fields indexed as full-text in RediSearch; empty disables search
	HTTP         HTTP
}

// HTTP groups the settings of the read-only HTTP API (nt-sync -serve).
type HTTP struct {
	Addr          string   // listen address; loopback by default, TLS and exposure belong to the reverse proxy
	ConsumersFile string   // JSON file with the projects allowed to call the API
	PriceFields   []string // row fields hidden from the consumers that may not see prices
	Fields        []string // columns the API returns (NT_API_FIELDS); nil, from "*", returns every column
}

// Redis groups the Redis connection settings.
type Redis struct {
	Addr     string
	Password string
	DB       int
	TLS      bool
	Prefix   string // namespace of every key
	Keep     int    // published versions kept in Redis (the current one plus the previous ones)

	ChangesMaxLen int64 // entries kept in the <prefix>:cambios stream; 0 turns the change stream off
}

// Load builds a Config from the given lookup function (os.LookupEnv in production).
func Load(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	var errs []error
	need := func(key string) string {
		v := get(key, "")
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", key))
		}
		return v
	}

	c := Config{
		APIURL:     need("NT_API_URL"),
		APIKey:     need("NT_API_KEY"),
		SQLitePath: get("SQLITE_PATH", "data/nt.db"),
		Redis: Redis{
			Addr:     need("REDIS_ADDR"),
			Password: get("REDIS_PASSWORD", ""),
			Prefix:   get("REDIS_PREFIX", "nt"),
		},
	}

	var err error
	if c.PollInterval, err = time.ParseDuration(get("POLL_INTERVAL", "30s")); err != nil || c.PollInterval < time.Second {
		errs = append(errs, errors.New("POLL_INTERVAL must be a duration of at least 1s, e.g. 30s"))
	}
	if c.Redis.DB, err = strconv.Atoi(get("REDIS_DB", "0")); err != nil || c.Redis.DB < 0 {
		errs = append(errs, errors.New("REDIS_DB must be a non-negative integer"))
	}
	if c.Redis.TLS, err = strconv.ParseBool(get("REDIS_TLS", "false")); err != nil {
		errs = append(errs, errors.New("REDIS_TLS must be true or false"))
	}
	if c.Redis.Keep, err = strconv.Atoi(get("REDIS_KEEP_VERSIONS", "2")); err != nil || c.Redis.Keep < 1 {
		errs = append(errs, errors.New("REDIS_KEEP_VERSIONS must be an integer of at least 1"))
	}
	if c.Redis.ChangesMaxLen, err = strconv.ParseInt(get("NT_CHANGES_MAXLEN", "100000"), 10, 64); err != nil || c.Redis.ChangesMaxLen < 0 {
		errs = append(errs, errors.New("NT_CHANGES_MAXLEN must be a non-negative integer (0 turns the change stream off)"))
	}
	for _, f := range strings.Split(get("NT_INDEX_FIELDS", "cum_facturar,cod_auf,cod_mutual,estado,contrato"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			c.IndexFields = append(c.IndexFields, f)
		}
	}
	for _, f := range strings.Split(get("NT_SEARCH_FIELDS", ""), ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		if !validFieldName(f) {
			errs = append(errs, fmt.Errorf("NT_SEARCH_FIELDS: %q is not a valid field name (letters, digits and _ only)", f))
			continue
		}
		c.SearchFields = append(c.SearchFields, f)
	}
	c.HTTP.Addr = get("NT_HTTP_ADDR", "127.0.0.1:8080")
	c.HTTP.ConsumersFile = get("NT_CONSUMERS_FILE", "consumers.json")
	for _, f := range strings.Split(get("NT_PRICE_FIELDS", "precio_venta,precio_maximo"), ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		if !validFieldName(f) {
			errs = append(errs, fmt.Errorf("NT_PRICE_FIELDS: %q is not a valid field name (letters, digits and _ only)", f))
			continue
		}
		c.HTTP.PriceFields = append(c.HTTP.PriceFields, f)
	}
	var fieldsErr error
	if c.HTTP.Fields, fieldsErr = parseAPIFields(get("NT_API_FIELDS", defaultAPIFields)); fieldsErr != nil {
		errs = append(errs, fieldsErr)
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, nil
}

// defaultAPIFields are the columns the HTTP API returns unless NT_API_FIELDS says otherwise.
const defaultAPIFields = "id,cum_facturar,contrato,contrato Evento,tipo_producto,descripcion,cod_gen_siesa,cod_mutual,activo," +
	"ffm_rips,umm_rips,upr_rips,descripcion_siesa,atc,registro_sanitario,estado_registro,clasificacion_entrega,plu,subplan,precio_maximo"

// parseAPIFields reads NT_API_FIELDS: a comma separated list of exact column names (a name may contain
// spaces) or "*" for every column. A list with no column at all is an error, because an empty list
// must not turn into "every column" by accident.
func parseAPIFields(raw string) ([]string, error) {
	var out []string
	for _, f := range strings.Split(raw, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	switch {
	case len(out) == 0:
		return nil, errors.New("NT_API_FIELDS must list at least one column, or be * for every column")
	case slices.Contains(out, "*") && len(out) > 1:
		return nil, errors.New("NT_API_FIELDS: * means every column and cannot be mixed with column names")
	case out[0] == "*":
		return nil, nil
	}
	return out, nil
}

// validFieldName accepts the characters of a plain JSON key, so a field can be used in a
// JSON path and as a RediSearch attribute name without any escaping.
func validFieldName(s string) bool {
	for _, r := range s {
		if r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return s != ""
}

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }
