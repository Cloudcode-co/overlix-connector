// Package account signs the connector in to Overlix (identity service) and keeps the
// token pair fresh. Refresh tokens rotate: every refresh writes the new pair to disk.
package account

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultIdentity is the identity API base.
const DefaultIdentity = "https://id.overlix.net/api/v1"

// Tokens is what the token file holds (a secret: 0600, never logged).
type Tokens struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// Account owns the token file.
type Account struct {
	base string
	file string
	mu   sync.Mutex
	tok  Tokens
	http *http.Client
}

var ErrSignedOut = errors.New("session expired: run `overlix-connector login` again")

// Open loads the token file written by Login.
func Open(base, file string) (*Account, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read %s (run `overlix-connector login` first): %w", file, err)
	}
	a := &Account{base: base, file: file, http: &http.Client{Timeout: 20 * time.Second}}
	if err := json.Unmarshal(b, &a.tok); err != nil {
		return nil, fmt.Errorf("token file %s is damaged: %w", file, err)
	}
	return a, nil
}

// Login signs in with email and password and writes the token file.
func Login(ctx context.Context, base, file, email, password string) (*Account, error) {
	a := &Account{base: base, file: file, http: &http.Client{Timeout: 20 * time.Second}}
	if err := a.call(ctx, "/auth/login", map[string]string{"email": email, "password": password}); err != nil {
		return nil, err
	}
	return a, nil
}

// Access returns a valid access token, refreshing it when it expires within 5 minutes.
func (a *Account) Access(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tok.AccessToken != "" && time.Until(a.tok.ExpiresAt) > 5*time.Minute {
		return a.tok.AccessToken, nil
	}
	if a.tok.RefreshToken == "" {
		return "", ErrSignedOut
	}
	if err := a.callLocked(ctx, "/auth/refresh", map[string]string{"refresh_token": a.tok.RefreshToken}); err != nil {
		return "", err
	}
	return a.tok.AccessToken, nil
}

func (a *Account) call(ctx context.Context, path string, body any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.callLocked(ctx, path, body)
}

func (a *Account) callLocked(ctx context.Context, path string, body any) error {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Device-Name", "Overlix Connector")
	req.Header.Set("X-Device-Platform", "server")
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		Code         string `json:"code"`
		Message      string `json:"message"`
		Error        string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode == http.StatusUnauthorized && path == "/auth/refresh" {
		return ErrSignedOut
	}
	if resp.StatusCode/100 != 2 || out.AccessToken == "" {
		msg := out.Message
		if msg == "" {
			msg = out.Error
		}
		return fmt.Errorf("identity %s: %d %s %s", path, resp.StatusCode, out.Code, msg)
	}
	exp := out.ExpiresIn
	if exp <= 0 {
		exp = 15 * 60
	}
	a.tok = Tokens{AccessToken: out.AccessToken, RefreshToken: out.RefreshToken, ExpiresAt: time.Now().Add(time.Duration(exp) * time.Second)}
	if a.tok.RefreshToken == "" {
		return errors.New("identity returned no refresh token")
	}
	return a.save()
}

func (a *Account) save() error {
	raw, _ := json.MarshalIndent(a.tok, "", "  ")
	if err := os.MkdirAll(filepath.Dir(a.file), 0o700); err != nil {
		return err
	}
	tmp := a.file + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, a.file)
}
