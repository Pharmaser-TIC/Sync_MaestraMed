// Package api is the HTTP client of the medication-creation API (nt_version and nt_snapshot).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"nt-sync/internal/model"
)

const (
	versionTimeout  = 30 * time.Second
	snapshotTimeout = 5 * time.Minute
)

// Client calls the API authenticating with the X-API-Key header.
type Client struct {
	baseURL string
	key     string
	http    *http.Client
}

// New returns a client for the api.php URL. Gzip responses are decoded transparently by net/http.
func New(baseURL, key string) *Client {
	return &Client{baseURL: baseURL, key: key, http: &http.Client{}}
}

// Version asks whether anything changed; it is cheap on the server side.
func (c *Client) Version(ctx context.Context) (model.VersionInfo, error) {
	var v model.VersionInfo
	if err := c.get(ctx, "nt_version", versionTimeout, &v); err != nil {
		return v, err
	}
	if v.Version == "" {
		return v, errors.New("nt_version answered without a version")
	}
	return v, nil
}

// Snapshot downloads every calculated row. The server calculates once per call.
func (c *Client) Snapshot(ctx context.Context) (model.Snapshot, error) {
	var s model.Snapshot
	if err := c.get(ctx, "nt_snapshot", snapshotTimeout, &s); err != nil {
		return s, err
	}
	if s.Version == "" {
		return s, errors.New("nt_snapshot answered without a version")
	}
	return s, nil
}

func (c *Client) get(ctx context.Context, action string, timeout time.Duration, out any) error {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return fmt.Errorf("invalid API URL: %w", err)
	}
	q := u.Query()
	q.Set("accion", action)
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.key)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", action, redactURL(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = json.Unmarshal(body, &e)
		if e.Error == "" {
			e.Error = "no error message"
		}
		return fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, e.Error)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%s: invalid JSON answer: %w", action, err)
	}
	return nil
}

// redactURL keeps transport errors from echoing the full request URL.
func redactURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
