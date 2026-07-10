package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"golang.org/x/sync/singleflight"
)

// tokenFetcher obtains a JWT for the given credentials.
type tokenFetcher interface {
	authenticate(ctx context.Context, username, password string) (string, error)
}

// tokenCache stores JWTs keyed by a hash of the client's credentials.
//
// The cache key is derived from both username AND password so that a changed
// or wrong password never reuses a previously issued token: a different
// password yields a different key and is re-validated by the auth endpoint.
//
// singleflight collapses concurrent misses for the same key into a single
// call to the auth endpoint, preventing a thundering herd on cache-empty
// windows (startup, or right after a 401 invalidation).
type tokenCache struct {
	fetcher tokenFetcher

	mu     sync.RWMutex
	tokens map[string]string

	group singleflight.Group
}

func newTokenCache(fetcher tokenFetcher) *tokenCache {
	return &tokenCache{
		fetcher: fetcher,
		tokens:  make(map[string]string),
	}
}

// cacheKey derives an opaque key from credentials. Raw credentials are never
// used as the map key, to reduce their exposure in memory dumps.
func cacheKey(username, password string) string {
	sum := sha256.Sum256([]byte(username + ":" + password))
	return hex.EncodeToString(sum[:])
}

// get returns a cached JWT for the credentials, fetching one if absent.
// Concurrent callers with the same credentials share a single fetch.
func (c *tokenCache) get(ctx context.Context, username, password string) (string, error) {
	key := cacheKey(username, password)

	c.mu.RLock()
	tok, ok := c.tokens[key]
	c.mu.RUnlock()
	if ok {
		return tok, nil
	}

	return c.fetch(ctx, key, username, password)
}

// fetch obtains a fresh token via singleflight and stores it.
func (c *tokenCache) fetch(ctx context.Context, key, username, password string) (string, error) {
	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		// Re-check under the group: an earlier concurrent caller may have
		// already populated the cache before this fetch acquired the flight.
		c.mu.RLock()
		tok, ok := c.tokens[key]
		c.mu.RUnlock()
		if ok {
			return tok, nil
		}

		// Detach from the triggering caller's context. singleflight runs this
		// function on whichever caller happened to win the flight. If that
		// caller cancels or times out, its context would otherwise propagate
		// the failure to every other caller waiting on the same key. The auth
		// HTTP client's own Timeout still bounds this call, so it cannot hang.
		tok, err := c.fetcher.authenticate(context.WithoutCancel(ctx), username, password)
		if err != nil {
			return "", err
		}

		c.mu.Lock()
		c.tokens[key] = tok
		c.mu.Unlock()

		return tok, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// invalidate drops the cached token for the given credentials, forcing the
// next get to re-authenticate. Called after an upstream 401.
func (c *tokenCache) invalidate(username, password string) {
	key := cacheKey(username, password)
	c.mu.Lock()
	delete(c.tokens, key)
	c.mu.Unlock()
}
