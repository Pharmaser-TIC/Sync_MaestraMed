package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Serve runs the handler on addr until ctx is cancelled, then shuts down gracefully.
// Timeouts keep slow or stuck clients from holding connections open.
func Serve(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("http api listening", "addr", addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			return err
		}
		if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
