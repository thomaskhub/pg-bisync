// Package watch turns the status of a node into alerts: one message when a problem starts (after a
// grace period where it can be transient) and one when it is resolved.
package watch

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/thomaskhub/pg-bisync/internal/config"
	"github.com/thomaskhub/pg-bisync/internal/notify"
	"github.com/thomaskhub/pg-bisync/internal/status"
)

// Thresholds of the rules.
type Thresholds struct {
	Grace       time.Duration // stall / inactive / unreachable must last this long
	LagBytes    int64         // slot lag on either side
	ClockMillis float64       // clock difference to the peer
}

// State carries active incidents between evaluations.
type State struct {
	Active    map[string]string    // condition → alert text
	Since     map[string]time.Time // condition first seen (grace)
	Conflicts int64                // last seen open conflicts (-1 = not yet known)
}

// NewState returns the state before the first evaluation.
func NewState() State {
	return State{Active: map[string]string{}, Since: map[string]time.Time{}, Conflicts: -1}
}

type condition struct {
	key   string
	on    bool
	grace bool
	text  string
}

// Evaluate applies the rules to one status reading (err: status could not be read at all).
func Evaluate(st State, node string, r status.Report, err error, now time.Time, th Thresholds) (State, []string) {
	var conds []condition
	if err != nil {
		conds = append(conds, condition{"unreachable", true, true, "cannot read local status: " + err.Error()})
	} else {
		peerDown := strings.HasPrefix(r.Message, "peer unreachable")
		conds = append(conds,
			condition{"unreachable", peerDown, true, r.Message},
			condition{"stalled", r.State == "stalled" && !peerDown, true, "sync stalled: " + r.Message},
			condition{"inactive", !peerDown && !r.SlotOnPeerActive && r.SlotOnPeerLag != nil, true, "worker not connected to the peer (slot inactive)"},
			condition{"slot_lost", r.SlotOnPeerWAL == "lost" || r.SlotOnPeerWAL == "unreserved", false,
				"slot on the peer is " + r.SlotOnPeerWAL + ": rebuild this node (TRUNCATE + init)"},
			condition{"lag", over(r.SlotOnPeerLag, th.LagBytes) || over(r.BacklogForPeerLag, th.LagBytes), false,
				fmt.Sprintf("lag over %d MB (from peer %s, for peer %s)", th.LagBytes>>20, mb(r.SlotOnPeerLag), mb(r.BacklogForPeerLag))},
			condition{"clock", r.ClockDiffMillis != nil && math.Abs(*r.ClockDiffMillis) > th.ClockMillis, false,
				fmt.Sprintf("clock differs from the peer by %.0f ms (last change wins needs synced clocks)", deref(r.ClockDiffMillis))},
		)
	}
	var msgs []string
	for _, c := range conds {
		msgs = append(msgs, st.apply(node, c, now, th.Grace)...)
	}
	if err == nil {
		switch {
		case st.Conflicts < 0 && r.OpenConflicts > 0:
			msgs = append(msgs, fmt.Sprintf("⚠️ %s: %d open conflicts (bisync.conflict)", node, r.OpenConflicts))
		case st.Conflicts >= 0 && r.OpenConflicts > st.Conflicts:
			msgs = append(msgs, fmt.Sprintf("⚠️ %s: %d new conflicts, %d open (bisync.conflict)", node, r.OpenConflicts-st.Conflicts, r.OpenConflicts))
		}
		st.Conflicts = r.OpenConflicts
	}
	return st, msgs
}

func (st State) apply(node string, c condition, now time.Time, grace time.Duration) []string {
	_, active := st.Active[c.key]
	if !c.on {
		delete(st.Since, c.key)
		if active {
			delete(st.Active, c.key)
			return []string{fmt.Sprintf("✅ %s: resolved: %s", node, c.key)}
		}
		return nil
	}
	if active {
		return nil
	}
	first, seen := st.Since[c.key]
	if !seen {
		st.Since[c.key] = now
		first = now
	}
	if c.grace && now.Sub(first) < grace {
		return nil
	}
	st.Active[c.key] = c.text
	return []string{fmt.Sprintf("⚠️ %s: %s", node, c.text)}
}

func over(v *int64, limit int64) bool { return v != nil && limit > 0 && *v > limit }

func mb(v *int64) string {
	if v == nil {
		return "?"
	}
	return fmt.Sprintf("%d MB", *v>>20)
}

func deref(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

// Run evaluates the node's status every interval and sends the messages until ctx ends.
func Run(ctx context.Context, cfg config.Config, n notify.Notifier, log *slog.Logger) error {
	a := cfg.Alerts
	th := Thresholds{Grace: a.Grace, LagBytes: a.LagBytes, ClockMillis: a.ClockMillis}
	st := NewState()
	tick := time.NewTicker(a.Interval)
	defer tick.Stop()
	for {
		r, err := status.Run(ctx, cfg)
		var msgs []string
		st, msgs = Evaluate(st, cfg.Node, r, err, time.Now(), th)
		for _, m := range msgs {
			log.Info("alert", "text", m)
			if err := n.Notify(ctx, m); err != nil {
				log.Error("notify failed", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}
