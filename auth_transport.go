package main

import (
	"fmt"
	"io"
	"net/http"
)

// authTransport exchanges the request's Basic credentials for a JWT, sets the
// Bearer header, and forwards to the upstream. On a 401 it invalidates the
// cached token and retries once with a freshly fetched token.
type authTransport struct {
	base  http.RoundTripper
	cache *tokenCache
}

func newAuthTransport(base http.RoundTripper, cache *tokenCache) *authTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &authTransport{base: base, cache: cache}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	username, password, ok := basicAuth(req)
	if !ok {
		return nil, &authError{StatusCode: http.StatusUnauthorized}
	}

	token, err := t.cache.get(req.Context(), username, password)
	if err != nil {
		return nil, err
	}

	resp, err := t.do(req, token)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	// Upstream rejected the token (likely expired). Drain and close the first
	// response body, invalidate, re-fetch once, and retry.
	drain(resp.Body)

	t.cache.invalidate(username, password)
	token, err = t.cache.get(req.Context(), username, password)
	if err != nil {
		return nil, err
	}

	return t.do(req, token)
}

// do clones the request, swaps Basic for Bearer, and sends it upstream.
// The inbound Authorization header (Basic) must not reach Airflow.
func (t *authTransport) do(req *http.Request, token string) (*http.Response, error) {
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+token)

	// A retry needs a fresh body reader. GetBody is populated by the stdlib
	// for the common cases. If a body exists without GetBody we cannot safely
	// retry, but the first attempt still works.
	if req.Body != nil && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("rewind request body: %w", err)
		}
		out.Body = body
	}

	return t.base.RoundTrip(out)
}

// basicAuth reads Basic credentials from the request. It exists as a named
// helper so the transport reads the same way whether it is handed a server
// request or a transport-level request.
func basicAuth(req *http.Request) (string, string, bool) {
	return req.BasicAuth()
}

func drain(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
	_ = body.Close()
}
