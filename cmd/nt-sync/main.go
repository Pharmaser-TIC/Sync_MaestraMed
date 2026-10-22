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
	"syscall"

	"github.com/redis/go-redis/v9"

	"nt-sync/internal/api"
	"nt-sync/internal/config"
	"nt-sync/internal/publish"
	"nt-sync/internal/store"
	"nt-sync/internal/syncer"
)

func main() {
	once := flag.Bool("once", false, "run a single synchronization cycle and exit")
	envFile := flag.String("env", ".env", "file with KEY=VALUE settings; variables already set in the environment win; a missing file is ignored")
	debug := flag.Bool("debug", false, "log every cycle, including the ones without changes")
	check := flag.Bool("check", false, "read back what is stored in Redis, print a report and exit (1 when something is wrong); does not call the API")
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

	if err := run(log, *once, *check); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func newRedis(r config.Redis) *redis.Client {
	opts := &redis.Options{Addr: r.Addr, Password: r.Password, DB: r.DB}
	if r.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return redis.NewClient(opts)
}

// checkRedis prints what is stored in Redis; it never writes and never touches SQLite or the API.
func checkRedis(ctx context.Context, rdb *redis.Client, cfg config.Redis) error {
	fmt.Printf("Redis %s (db %d, tls %t), prefix %q: connection OK\n\n", cfg.Addr, cfg.DB, cfg.TLS, cfg.Prefix)
	report, err := publish.New(rdb, cfg.Prefix, cfg.Keep).Inspect(ctx)
	if err != nil {
		return fmt.Errorf("reading redis: %w", err)
	}
	report.Write(os.Stdout)
	if !report.OK() {
		return fmt.Errorf("check found %d problem(s)", len(report.Problems))
	}
	return nil
}

func run(log *slog.Logger, once, check bool) error {
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
	if check {
		return checkRedis(ctx, rdb, cfg.Redis)
	}

	st, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("opening local store: %w", err)
	}
	defer st.Close()

	s := &syncer.Syncer{
		Source:      api.New(cfg.APIURL, cfg.APIKey),
		Store:       st,
		Publisher:   publish.New(rdb, cfg.Redis.Prefix, cfg.Redis.Keep),
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
