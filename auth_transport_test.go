package main

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// roundTripFunc adapts a function to http.RoundTripper so the transport's own
// logic (Basic->Bearer swap, retry-on-401, body rewind) can be tested against
// canned upstream responses without opening a socket. The upstream's real
// behaviour is covered separately by the Python test in e2e/test_e2e.py.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newResp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func newAuthReq(method, url string, body io.Reader, user, pass string) *http.Request {
	req, _ := http.NewRequest(method, url, body)
	req.SetBasicAuth(user, pass)
	return req
}

// TestSwapsBasicForBearer: the inbound Basic credentials are exchanged for a
// token and the upstream sees only a Bearer header.
func TestSwapsBasicForBearer(t *testing.T) {
	f := &stubFetcher{}
	cache := newTokenCache(f)

	var seenAuth string
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seenAuth = r.Header.Get("Authorization")
		return newResp(http.StatusOK, "ok"), nil
	})
	tr := newAuthTransport(base, cache)

	resp, err := tr.RoundTrip(newAuthReq(http.MethodGet, "http://airflow/api/v2/dags", nil, "user-a", "pass-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if want := "Bearer tok-user-a:pass-a"; seenAuth != want {
		t.Fatalf("upstream Authorization: want %q, got %q", want, seenAuth)
	}
	if strings.HasPrefix(seenAuth, "Basic ") {
		t.Fatalf("inbound Basic header must not reach the upstream, got %q", seenAuth)
	}
	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("want 1 token fetch, got %d", got)
	}
}

// TestTransportRejectsMissingCredentials: with no Basic credentials the
// transport fails closed with an authError and never touches the upstream.
func TestTransportRejectsMissingCredentials(t *testing.T) {
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("upstream must not be called without credentials")
		return nil, nil
	})
	tr := newAuthTransport(base, newTokenCache(&stubFetcher{}))

	req, _ := http.NewRequest(http.MethodGet, "http://airflow/api/v2/dags", nil)
	_, err := tr.RoundTrip(req)

	var ae *authError
	if !errors.As(err, &ae) {
		t.Fatalf("want authError, got %v", err)
	}
}

// TestRetryOn401RefetchesAndReplays: a 401 from the upstream triggers exactly
// one cache invalidation, re-fetch, and replay.
func TestRetryOn401RefetchesAndReplays(t *testing.T) {
	f := &stubFetcher{}
	cache := newTokenCache(f)

	var attempts int32
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return newResp(http.StatusUnauthorized, "expired"), nil
		}
		return newResp(http.StatusOK, "ok"), nil
	})
	tr := newAuthTransport(base, cache)

	resp, err := tr.RoundTrip(newAuthReq(http.MethodGet, "http://airflow/api/v2/dags", nil, "user-a", "pass-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 after retry, got %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("want 2 upstream attempts (initial + retry), got %d", got)
	}
	if got := atomic.LoadInt32(&f.calls); got != 2 {
		t.Fatalf("want 2 token fetches (initial + re-fetch on 401), got %d", got)
	}
}

// TestNoRetryOnSuccess: a non-401 response is returned as-is, without a second
// upstream attempt or a second token fetch.
func TestNoRetryOnSuccess(t *testing.T) {
	f := &stubFetcher{}
	cache := newTokenCache(f)

	var attempts int32
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		atomic.AddInt32(&attempts, 1)
		return newResp(http.StatusInternalServerError, "boom"), nil
	})
	tr := newAuthTransport(base, cache)

	resp, err := tr.RoundTrip(newAuthReq(http.MethodGet, "http://airflow/api/v2/dags", nil, "user-a", "pass-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want 500 passed through, got %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("non-401 must not retry; want 1 attempt, got %d", got)
	}
}

// TestRetryRewindsRequestBody: a request with a body is replayed intact on the
// post-401 retry (GetBody rewind).
func TestRetryRewindsRequestBody(t *testing.T) {
	f := &stubFetcher{}
	cache := newTokenCache(f)

	const payload = "trigger-run-payload"
	var bodies []string
	var attempts int32
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&attempts, 1)
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if n == 1 {
			return newResp(http.StatusUnauthorized, ""), nil
		}
		return newResp(http.StatusOK, "ok"), nil
	})
	tr := newAuthTransport(base, cache)

	req := newAuthReq(http.MethodPost, "http://airflow/api/v2/dags/x/dagRuns", strings.NewReader(payload), "user-a", "pass-a")
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if len(bodies) != 2 {
		t.Fatalf("want 2 upstream attempts, got %d", len(bodies))
	}
	if bodies[0] != payload || bodies[1] != payload {
		t.Fatalf("request body not rewound on retry: %q", bodies)
	}
}
