package main

import (
	"flag"
	"fmt"
	"net/url"
	"time"
)

// Config holds the proxy runtime configuration.
//
// TLS is intentionally not handled here: termination is delegated to the
// ingress (cert-manager). This proxy listens in plaintext and is expected to
// run behind that ingress, never exposed directly.
type Config struct {
	ListenAddr string
	AirflowURL *url.URL
	TokenPath  string
	Timeout    time.Duration
}

func parseConfig() (*Config, error) {
	var (
		listenAddr = flag.String("listen", ":8080", "address to listen on (plaintext; TLS is terminated at the ingress)")
		airflowRaw = flag.String("airflow-url", "", "base URL of the Airflow instance (required), e.g. https://airflow.internal")
		tokenPath  = flag.String("token-path", "/auth/token", "path to the Airflow auth token endpoint")
		timeout    = flag.Duration("timeout", 30*time.Second, "upstream request timeout")
	)
	flag.Parse()

	if *airflowRaw == "" {
		return nil, fmt.Errorf("-airflow-url is required")
	}

	u, err := url.Parse(*airflowRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid -airflow-url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("-airflow-url must include scheme and host, e.g. https://airflow.internal")
	}

	return &Config{
		ListenAddr: *listenAddr,
		AirflowURL: u,
		TokenPath:  *tokenPath,
		Timeout:    *timeout,
	}, nil
}
