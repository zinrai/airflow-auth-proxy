package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The handler tests use httptest.NewRecorder (in-memory, no socket) to exercise
// the proxy's own concerns: the Basic-credentials gate and the error-to-status
// mapping. A stub RoundTripper stands in for the upstream so no server is
// needed. The real relay is covered end to end by the Python test in
// e2e/test_e2e.py.

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// TestMissingCredentialsGate: a request without Basic credentials is rejected
// with 401 and a Basic challenge, before the upstream is ever reached.
func TestMissingCredentialsGate(t *testing.T) {
	h := newProxyHandler(mustURL(t, "http://airflow.internal"),
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("upstream must not be reached without credentials")
			return nil, nil
		}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://proxy/api/v2/dags", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Fatalf("want a Basic challenge, got %q", got)
	}
}

// TestErrorMappingAuthErrorTo401: an authError from the transport surfaces to
// the client as 401 (credentials rejected), not a generic gateway error.
func TestErrorMappingAuthErrorTo401(t *testing.T) {
	h := newProxyHandler(mustURL(t, "http://airflow.internal"),
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, &authError{StatusCode: http.StatusUnauthorized}
		}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://proxy/api/v2/dags", nil)
	req.SetBasicAuth("user-a", "pass-a")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for authError, got %d", rec.Code)
	}
}

// TestErrorMappingOtherTo502: any non-auth transport error (e.g. the backend is
// down) is mapped to 502 Bad Gateway.
func TestErrorMappingOtherTo502(t *testing.T) {
	h := newProxyHandler(mustURL(t, "http://airflow.internal"),
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp: connection refused")
		}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://proxy/api/v2/dags", nil)
	req.SetBasicAuth("user-a", "pass-a")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("want 502 for a generic transport error, got %d", rec.Code)
	}
}

// TestHandlerRelaysUpstreamResponse: the one in-memory wiring check that a
// credentialed request is forwarded through the transport and the upstream
// response (status + body) is relayed back to the client.
func TestHandlerRelaysUpstreamResponse(t *testing.T) {
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return newResp(http.StatusOK, `{"dags":[]}`), nil
	})
	transport := newAuthTransport(base, newTokenCache(&stubFetcher{}))
	h := newProxyHandler(mustURL(t, "http://airflow.internal"), transport)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://proxy/api/v2/dags", nil)
	req.SetBasicAuth("user-a", "pass-a")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != `{"dags":[]}` {
		t.Fatalf("upstream body not relayed, got %q", body)
	}
}
