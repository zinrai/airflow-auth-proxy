package main

import (
	"errors"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// proxyHandler forwards incoming requests to Airflow. Authentication is handled
// entirely by authTransport, so the handler itself only validates that Basic
// credentials are present and strips the inbound Authorization header (the
// transport sets the Bearer header instead).
type proxyHandler struct {
	proxy *httputil.ReverseProxy
}

func newProxyHandler(target *url.URL, transport http.RoundTripper) *proxyHandler {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			var ae *authError
			if errors.As(err, &ae) {
				http.Error(w, "authentication failed", http.StatusUnauthorized)
				return
			}
			log.Printf("proxy error: %v", err)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
	return &proxyHandler{proxy: rp}
}

func (h *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := r.BasicAuth(); !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="airflow"`)
		http.Error(w, "missing basic credentials", http.StatusUnauthorized)
		return
	}
	h.proxy.ServeHTTP(w, r)
}
