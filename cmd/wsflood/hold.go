package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// holdResult is what one long-lived reader saw.
type holdResult struct {
	channel string
	events  int64
	err     error // non-nil when the connection ended before the hold window did
}

// runWSHold keeps n readers subscribed (round-robin over the three channels) for the whole duration,
// as a soak test's background WebSocket load. It fails if any reader is disconnected early or receives
// no event at all, so a hub that leaks, stalls or drops healthy clients under hours of traffic shows up.
func runWSHold(ctx context.Context, baseURL string, n, dialWorkers int, d time.Duration, out io.Writer) error {
	channels := []string{"sms", "email", "push"}
	conns := make([]*websocket.Conn, 0, n)
	chans := make([]string, 0, n)
	for i, ch := range channels {
		k := n / len(channels)
		if i < n%len(channels) {
			k++
		}
		cs, err := dialWSBatch(ctx, toWSURL(baseURL, "/api/v1/ws?channel="+ch), k, dialWorkers, 0)
		if err != nil {
			closeAllWS(conns)
			return fmt.Errorf("dialing %s readers: %w", ch, err)
		}
		for range cs {
			chans = append(chans, ch)
		}
		conns = append(conns, cs...)
	}
	_, _ = fmt.Fprintf(out, "ws-hold: %d readers connected for %s\n", len(conns), d)

	holdCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	results := make([]holdResult, len(conns))
	var total atomic.Int64
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := holdResult{channel: chans[i]}
			for {
				_, _, err := c.Read(holdCtx)
				if err != nil {
					if holdCtx.Err() == nil {
						r.err = err
					}
					break
				}
				r.events++
				total.Add(1)
			}
			results[i] = r
		}()
	}
	wg.Wait()
	closeAllWS(conns)

	var early, silent int
	perChannel := map[string]int64{}
	var firstErr error
	for _, r := range results {
		perChannel[r.channel] += r.events
		if r.err != nil {
			early++
			if firstErr == nil {
				firstErr = r.err
			}
		}
		if r.events == 0 {
			silent++
		}
	}
	_, _ = fmt.Fprintf(out, "ws-hold: %d events (sms %d, email %d, push %d); disconnected early: %d; received nothing: %d\n",
		total.Load(), perChannel["sms"], perChannel["email"], perChannel["push"], early, silent)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if early > 0 || silent > 0 {
		return errors.Join(fmt.Errorf("%d readers disconnected early, %d received nothing", early, silent), firstErr)
	}
	_, _ = fmt.Fprintln(out, "ws-hold: PASS")
	return nil
}
