// Package main implements cmd/healthcheck, a minimal static HTTP GET probe for distroless
// containers where curl, wget, and shells are intentionally absent.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

const probeTimeout = 2 * time.Second

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url>")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	client := &http.Client{Timeout: probeTimeout}
	if err := probe(ctx, client, os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		os.Exit(1)
	}
}

func probe(ctx context.Context, client *http.Client, targetURL string) error {
	parsed, err := url.Parse(targetURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("invalid healthcheck URL %q", targetURL)
	}
	if client == nil {
		return errors.New("http client must not be nil")
	}

	//nolint:gosec // G704: CLI healthcheck probe intentionally targets the URL passed via argv.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return fmt.Errorf("creating healthcheck request: %w", err)
	}

	//nolint:gosec // G704: CLI healthcheck probe intentionally targets the validated URL above.
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("performing healthcheck request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}
