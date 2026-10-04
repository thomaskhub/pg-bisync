package watch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thomaskhub/pg-bisync/internal/status"
)

var th = Thresholds{Grace: 2 * time.Minute, LagBytes: 64 << 20, ClockMillis: 200}

func ok() status.Report {
	lag := int64(0)
	clock := 1.0
	return status.Report{Node: "eu", State: "running", SlotOnPeerActive: true, SlotOnPeerLag: &lag, BacklogForPeerLag: &lag, SlotOnPeerWAL: "reserved", ClockDiffMillis: &clock}
}

// step feeds readings at the given minutes and returns all messages per step.
func steps(t *testing.T, readings []func(status.Report) (status.Report, error)) [][]string {
	t.Helper()
	st := NewState()
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	var out [][]string
	for i, f := range readings {
		r, err := f(ok())
		var msgs []string
		st, msgs = Evaluate(st, "eu", r, err, t0.Add(time.Duration(i)*time.Minute), th)
		out = append(out, msgs)
	}
	return out
}

func same(r status.Report) (status.Report, error) { return r, nil }
func stalled(r status.Report) (status.Report, error) {
	r.State, r.Message = "stalled", "schema mismatch"
	return r, nil
}

func TestEvaluate(t *testing.T) {
	t.Run("W1_quiet_when_healthy", func(t *testing.T) {
		for i, m := range steps(t, []func(status.Report) (status.Report, error){same, same, same}) {
			if len(m) > 0 {
				t.Errorf("step %d: %v", i, m)
			}
		}
	})
	t.Run("W2_stall_after_grace_once_then_resolved", func(t *testing.T) {
		got := steps(t, []func(status.Report) (status.Report, error){stalled, stalled, stalled, stalled, same, same})
		want := []int{0, 0, 1, 0, 1, 0} // grace 2 min: alert at minute 2, nothing repeated, resolved once
		for i, n := range want {
			if len(got[i]) != n {
				t.Fatalf("step %d: %v", i, got[i])
			}
		}
		if !strings.Contains(got[2][0], "stalled") || !strings.Contains(got[4][0], "resolved: stalled") {
			t.Errorf("texts: %v / %v", got[2], got[4])
		}
	})
	t.Run("W3_short_stall_inside_grace_is_silent", func(t *testing.T) {
		for i, m := range steps(t, []func(status.Report) (status.Report, error){stalled, same, stalled, same}) {
			if len(m) > 0 {
				t.Errorf("step %d: %v", i, m)
			}
		}
	})
	t.Run("W4_lost_slot_immediately", func(t *testing.T) {
		got := steps(t, []func(status.Report) (status.Report, error){func(r status.Report) (status.Report, error) {
			r.SlotOnPeerWAL = "lost"
			return r, nil
		}})
		if len(got[0]) != 1 || !strings.Contains(got[0][0], "rebuild") {
			t.Errorf("%v", got[0])
		}
	})
	t.Run("W5_lag_and_clock", func(t *testing.T) {
		got := steps(t, []func(status.Report) (status.Report, error){func(r status.Report) (status.Report, error) {
			big := int64(100 << 20)
			skew := -350.0
			r.BacklogForPeerLag, r.ClockDiffMillis = &big, &skew
			return r, nil
		}})
		if len(got[0]) != 2 {
			t.Fatalf("%v", got[0])
		}
	})
	t.Run("W6_conflicts_baseline_then_new_ones", func(t *testing.T) {
		withConflicts := func(n int64) func(status.Report) (status.Report, error) {
			return func(r status.Report) (status.Report, error) { r.OpenConflicts = n; return r, nil }
		}
		got := steps(t, []func(status.Report) (status.Report, error){withConflicts(2), withConflicts(2), withConflicts(5), withConflicts(1)})
		if len(got[0]) != 1 || !strings.Contains(got[0][0], "2 open") {
			t.Errorf("start: %v", got[0])
		}
		if len(got[1]) != 0 || len(got[2]) != 1 || !strings.Contains(got[2][0], "3 new") || len(got[3]) != 0 {
			t.Errorf("later: %v", got)
		}
	})
	t.Run("W7_status_unreadable_after_grace", func(t *testing.T) {
		down := func(r status.Report) (status.Report, error) { return r, errors.New("connection refused") }
		got := steps(t, []func(status.Report) (status.Report, error){down, down, down, same})
		if len(got[0])+len(got[1]) != 0 || len(got[2]) != 1 || len(got[3]) != 1 {
			t.Errorf("%v", got)
		}
	})
	t.Run("W8_peer_down_reports_unreachable_not_stalled", func(t *testing.T) {
		peerDown := func(r status.Report) (status.Report, error) {
			r.State, r.Message = "stalled", "peer unreachable: dial tcp"
			return r, nil
		}
		got := steps(t, []func(status.Report) (status.Report, error){peerDown, peerDown, peerDown})
		if len(got[2]) != 1 || !strings.Contains(got[2][0], "peer unreachable") {
			t.Errorf("%v", got)
		}
	})
}
