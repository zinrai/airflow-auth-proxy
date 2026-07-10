package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// authClient obtains JWTs from the Airflow auth endpoint.
type authClient struct {
	httpClient *http.Client
	tokenURL   string
}

func newAuthClient(base *url.URL, tokenPath string, httpClient *http.Client) *authClient {
	ref := &url.URL{Path: tokenPath}
	return &authClient{
		httpClient: httpClient,
		tokenURL:   base.ResolveReference(ref).String(),
	}
}

type tokenRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

// authenticate exchanges username/password for a JWT access token.
func (a *authClient) authenticate(ctx context.Context, username, password string) (string, error) {
	body, err := json.Marshal(tokenRequest{Username: username, Password: password})
	if err != nil {
		return "", fmt.Errorf("marshal token request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tokenURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call token endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		// Do not include the response body: it may echo credentials.
		return "", &authError{StatusCode: resp.StatusCode}
	}

	var tr tokenResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tr); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("auth endpoint returned no access_token")
	}

	return tr.AccessToken, nil
}

// authError signals that the upstream auth endpoint rejected the credentials.
type authError struct {
	StatusCode int
}

func (e *authError) Error() string {
	return fmt.Sprintf("auth endpoint returned status %d", e.StatusCode)
}
