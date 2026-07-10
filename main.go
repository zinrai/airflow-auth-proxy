package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}

	// The auth (token) client is bounded by a whole-request Timeout: a token
	// exchange is a short unary call, never a stream, so capping it end to end
	// is safe and desirable.
	authHTTP := &http.Client{Timeout: cfg.Timeout}
	auth := newAuthClient(cfg.AirflowURL, cfg.TokenPath, authHTTP)
	cache := newTokenCache(auth)

	// The proxy transport must NOT use a whole-request Timeout: proxied API
	// responses may legitimately stream for a long time. Instead we bound only
	// time-to-first-response-byte via ResponseHeaderTimeout, which catches a
	// stalled upstream without truncating a long but healthy response body.
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.ResponseHeaderTimeout = cfg.Timeout
	transport := newAuthTransport(base, cache)
	handler := newProxyHandler(cfg.AirflowURL, transport)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Timeout,
		IdleTimeout:       cfg.Timeout,
		// WriteTimeout is intentionally unset: it would cap the whole response
		// and break streaming/long API responses. ResponseHeaderTimeout on the
		// upstream transport guards against a stalled backend instead.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s, forwarding to %s", cfg.ListenAddr, cfg.AirflowURL)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
