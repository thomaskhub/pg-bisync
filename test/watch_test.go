package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thomaskhub/pg-bisync/internal/notify"
	"github.com/thomaskhub/pg-bisync/internal/watch"
)

// TestWatch: a stall on eu is reported once after the grace period, and its resolution once.
func TestWatch(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]string
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		got = append(got, b["text"])
		mu.Unlock()
	}))
	defer srv.Close()
	cfg := c.cfg["eu"]
	cfg.Alerts.Interval, cfg.Alerts.Grace = 200*time.Millisecond, time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = watch.Run(ctx, cfg, notify.Webhook{URL: srv.URL}, logger()) }()
	defer func() { cancel(); <-done }()

	waitFor := func(substr string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			for _, m := range got {
				if strings.Contains(m, substr) {
					mu.Unlock()
					return
				}
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("no message containing %q, got %v", substr, got)
	}
	c.in.Exec(t, "ALTER TABLE project ADD COLUMN w text")
	c.in.Exec(t, "UPDATE project SET w = 'x' WHERE id = 'p1-1'")
	waitFor("sync stalled")
	c.eu.Exec(t, "ALTER TABLE project ADD COLUMN w text")
	waitFor("resolved: stalled")
	mu.Lock()
	defer mu.Unlock()
	n := 0
	for _, m := range got {
		if strings.Contains(m, "sync stalled") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("stall reported %d times, want once: %v", n, got)
	}
}
