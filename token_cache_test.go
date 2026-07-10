package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubFetcher is an in-memory tokenFetcher used to verify that the cache's
// features actually work: that a token is reused, that the cache key includes
// the password, that invalidation forces a re-fetch, and that a failed fetch is
// not cached. It stands in for the real auth client so these behaviours can be
// asserted directly, without an HTTP server.
type stubFetcher struct {
	calls   int32
	latency time.Duration
	err     error
}

func (s *stubFetcher) authenticate(ctx context.Context, username, password string) (string, error) {
	atomic.AddInt32(&s.calls, 1)
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if s.err != nil {
		return "", s.err
	}
	return "tok-" + username + ":" + password, nil
}

func TestCacheReusesToken(t *testing.T) {
	f := &stubFetcher{}
	c := newTokenCache(f)

	for i := 0; i < 5; i++ {
		tok, err := c.get(context.Background(), "user-a", "pass-a")
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if tok != "tok-user-a:pass-a" {
			t.Fatalf("get %d: unexpected token %q", i, tok)
		}
	}
	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("want 1 auth call across 5 gets, got %d", got)
	}
}

func TestCacheKeyIncludesPassword(t *testing.T) {
	f := &stubFetcher{}
	c := newTokenCache(f)

	if _, err := c.get(context.Background(), "user-a", "pass-a"); err != nil {
		t.Fatal(err)
	}
	// Same user, different password: must not reuse the first token.
	if _, err := c.get(context.Background(), "user-a", "pass-b"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&f.calls); got != 2 {
		t.Fatalf("different password must re-authenticate; want 2 calls, got %d", got)
	}
}

// blockingFetcher holds every fetch open until the test releases it, so the
// singleflight test can prove collapsing by construction (a single fetch is
// kept in flight while the other callers arrive) rather than relying on timing
// to make goroutines race.
type blockingFetcher struct {
	calls   int32
	entered chan struct{} // one signal per fetch that actually runs
	release chan struct{} // fetches block here until the test closes it
}

func (b *blockingFetcher) authenticate(ctx context.Context, username, password string) (string, error) {
	atomic.AddInt32(&b.calls, 1)
	b.entered <- struct{}{}
	<-b.release
	return "tok-" + username, nil
}

// TestSingleflightCollapsesConcurrentMisses verifies the singleflight feature
// works: while one fetch for a key is in flight, other callers for the same key
// share it instead of each triggering their own, and all receive that token.
func TestSingleflightCollapsesConcurrentMisses(t *testing.T) {
	const n = 8
	f := &blockingFetcher{
		entered: make(chan struct{}, n), // buffered so a stray fetch can't deadlock the assert
		release: make(chan struct{}),
	}
	c := newTokenCache(f)

	var wg sync.WaitGroup
	tokens := make([]string, n)
	errs := make([]error, n)

	// Leader triggers the flight and parks inside the (blocked) fetch.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tokens[0], errs[0] = c.get(context.Background(), "user-a", "pass-a")
	}()
	<-f.entered // the one fetch is now in flight and held open

	// Followers arrive while that fetch is still open, and each must join it.
	for i := 1; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tokens[i], errs[i] = c.get(context.Background(), "user-a", "pass-a")
		}()
	}

	// Let the followers register on the in-flight call, then complete the fetch
	// and hand its single result to everyone.
	time.Sleep(20 * time.Millisecond)
	close(f.release)
	wg.Wait()

	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("concurrent misses must collapse to a single fetch, got %d", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if tokens[i] != "tok-user-a" {
			t.Fatalf("caller %d got %q, want the one shared token", i, tokens[i])
		}
	}
}

func TestInvalidateForcesRefetch(t *testing.T) {
	f := &stubFetcher{}
	c := newTokenCache(f)

	if _, err := c.get(context.Background(), "user-a", "pass-a"); err != nil {
		t.Fatal(err)
	}
	c.invalidate("user-a", "pass-a")
	if _, err := c.get(context.Background(), "user-a", "pass-a"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&f.calls); got != 2 {
		t.Fatalf("invalidate must force a re-fetch; want 2 calls, got %d", got)
	}
}

func TestFetchErrorIsNotCached(t *testing.T) {
	sentinel := errors.New("auth down")
	f := &stubFetcher{err: sentinel}
	c := newTokenCache(f)

	if _, err := c.get(context.Background(), "user-a", "pass-a"); !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error, got %v", err)
	}
	// Recover and confirm the failed attempt left nothing cached.
	f.err = nil
	tok, err := c.get(context.Background(), "user-a", "pass-a")
	if err != nil {
		t.Fatalf("recovery get failed: %v", err)
	}
	if tok == "" {
		t.Fatal("expected a token after recovery")
	}
	if got := atomic.LoadInt32(&f.calls); got != 2 {
		t.Fatalf("failed fetch must not be cached; want 2 calls, got %d", got)
	}
}

// TestPeerCancellationDoesNotPoisonSingleflight verifies the context-detach
// fix: when the caller that happens to own the singleflight flight cancels,
// the other callers sharing that flight must still succeed.
func TestPeerCancellationDoesNotPoisonSingleflight(t *testing.T) {
	f := &stubFetcher{latency: 300 * time.Millisecond}
	c := newTokenCache(f)

	ctxA, cancelA := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	errs := make([]error, 3)

	// Caller A triggers the flight and will be cancelled mid-fetch.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errs[0] = c.get(ctxA, "user-a", "pass-a")
	}()

	// Give A time to enter the flight, then start peers B and C on the same key.
	time.Sleep(50 * time.Millisecond)
	wg.Add(2)
	for i := 1; i < 3; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, errs[i] = c.get(context.Background(), "user-a", "pass-a")
		}()
	}

	// Cancel A while B and C are still waiting on the shared flight.
	time.Sleep(30 * time.Millisecond)
	cancelA()
	wg.Wait()

	if errs[1] != nil || errs[2] != nil {
		t.Fatalf("peer cancellation poisoned waiters: b=%v c=%v", errs[1], errs[2])
	}
	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("want 1 auth call, got %d", got)
	}
}
