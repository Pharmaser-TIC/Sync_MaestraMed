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

	if err := run(log, *once); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, once bool) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	st, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("opening local store: %w", err)
	}
	defer st.Close()

	opts := &redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB}
	if cfg.Redis.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to redis: %w", err)
	}

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
