// Package config reads the service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
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
}

// Redis groups the Redis connection settings.
type Redis struct {
	Addr     string
	Password string
	DB       int
	TLS      bool
	Prefix   string // namespace of every key
	Keep     int    // published versions kept in Redis (the current one plus the previous ones)
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
	for _, f := range strings.Split(get("NT_INDEX_FIELDS", "cum_facturar,cod_auf,cod_mutual,estado,contrato"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			c.IndexFields = append(c.IndexFields, f)
		}
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, nil
}

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }
