// Command nt-sync keeps Redis in step with the Maestra Tarifaria published by the
// medication-creation API: it polls nt_version and, when the version changes, downloads
// nt_snapshot, keeps a local SQLite copy and publishes it to Redis.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"nt-sync/internal/api"
	"nt-sync/internal/config"
	"nt-sync/internal/httpapi"
	"nt-sync/internal/publish"
	"nt-sync/internal/store"
	"nt-sync/internal/syncer"
)

func main() {
	once := flag.Bool("once", false, "run a single synchronization cycle and exit")
	envFile := flag.String("env", ".env", "file with KEY=VALUE settings; variables already set in the environment win; a missing file is ignored")
	debug := flag.Bool("debug", false, "log every cycle, including the ones without changes")
	check := flag.Bool("check", false, "read back what is stored in Redis, print a report and exit (1 when something is wrong); does not call the API")
	serve := flag.Bool("serve", false, "serve the products over a read-only HTTP API (see NT_HTTP_ADDR, NT_CONSUMERS_FILE, NT_PRICE_FIELDS, NT_API_FIELDS); does not call the upstream API or synchronize")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	loaded, err := config.LoadDotEnv(*envFile)
	if err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
	log.Debug("settings file", "path", *envFile, "loaded", loaded)

	if *serve && (*once || *check) {
		log.Error("fatal", "error", "-serve cannot be combined with -once or -check")
		os.Exit(1)
	}
	if err := run(log, *once, *check, *serve); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func newRedis(r config.Redis) *redis.Client {
	// Publishing sends pipelines of thousands of commands; the 3s go-redis default is too
	// short over a VPN or a slow link and fails with "i/o timeout".
	opts := &redis.Options{
		Addr: r.Addr, Password: r.Password, DB: r.DB,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
	}
	if r.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return redis.NewClient(opts)
}

// checkRedis prints what is stored in Redis; it never writes and never touches SQLite or the API.
func checkRedis(ctx context.Context, rdb *redis.Client, cfg config.Redis, searchFields []string) error {
	fmt.Printf("Redis %s (db %d, tls %t), prefix %q: connection OK\n\n", cfg.Addr, cfg.DB, cfg.TLS, cfg.Prefix)
	report, err := publish.New(rdb, cfg.Prefix, cfg.Keep).WithSearch(searchFields).WithChanges(cfg.ChangesMaxLen).Inspect(ctx)
	if err != nil {
		return fmt.Errorf("reading redis: %w", err)
	}
	report.Write(os.Stdout)
	if !report.OK() {
		return fmt.Errorf("check found %d problem(s)", len(report.Problems))
	}
	return nil
}

// serveHTTP runs the read-only API. It never writes to Redis, so it can use a Redis user with read
// commands only, and it does not need the upstream API, the SQLite copy or the sync lock.
func serveHTTP(ctx context.Context, log *slog.Logger, cfg config.Config, pub *publish.Publisher) error {
	consumers, err := httpapi.LoadConsumers(cfg.HTTP.ConsumersFile)
	if err != nil {
		return err
	}
	h, err := httpapi.New(pub, httpapi.Options{
		Consumers:    consumers,
		PriceFields:  cfg.HTTP.PriceFields,
		Fields:       cfg.HTTP.Fields,
		IndexFields:  cfg.IndexFields,
		SearchFields: cfg.SearchFields,
		Log:          log,
	})
	if err != nil {
		return err
	}
	log.Info("serving", "consumers", len(consumers), "redis", cfg.Redis.Addr, "prefix", cfg.Redis.Prefix,
		"hidden_without_prices", strings.Join(cfg.HTTP.PriceFields, ","), "columns", columnsLog(cfg.HTTP.Fields))
	return httpapi.Serve(ctx, cfg.HTTP.Addr, h, log)
}

// columnsLog describes the columns the API returns, for the start-up log line.
func columnsLog(fields []string) string {
	if fields == nil {
		return "all"
	}
	return fmt.Sprintf("%d (%s)", len(fields), strings.Join(fields, ","))
}

func run(log *slog.Logger, once, check, serve bool) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := newRedis(cfg.Redis)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to redis: %w", err)
	}
	pub := publish.New(rdb, cfg.Redis.Prefix, cfg.Redis.Keep).WithSearch(cfg.SearchFields).WithChanges(cfg.Redis.ChangesMaxLen)
	if serve {
		return serveHTTP(ctx, log, cfg, pub)
	}
	if len(cfg.SearchFields) > 0 {
		if err := pub.CheckSearch(ctx); err != nil {
			return err
		}
	}
	if check {
		return checkRedis(ctx, rdb, cfg.Redis, cfg.SearchFields)
	}

	st, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("opening local store: %w", err)
	}
	defer st.Close()

	s := &syncer.Syncer{
		Source:      api.New(cfg.APIURL, cfg.APIKey),
		Store:       st,
		Publisher:   pub,
		IndexFields: cfg.IndexFields,
		Log:         log,
	}
	if once {
		return s.Once(ctx)
	}
	log.Info("started", "api", cfg.APIURL, "interval", cfg.PollInterval.String(), "redis", cfg.Redis.Addr, "prefix", cfg.Redis.Prefix)
	s.Run(ctx, cfg.PollInterval)
	log.Info("stopped")
	return nil
}
