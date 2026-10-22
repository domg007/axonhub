package biz

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func stickyCandidates(n int) []ModelStickyCandidate {
	result := make([]ModelStickyCandidate, n)
	for i := range result {
		result[i] = ModelStickyCandidate{ID: i + 1, Name: fmt.Sprint(i + 1)}
	}
	return result
}

func stickyAcquire(t *testing.T, m *ModelStickyManager, k ModelStickyKey, cs []ModelStickyCandidate, tried map[int]bool, fresh bool, want int) *ModelStickyLease {
	t.Helper()
	l, err := m.Acquire(context.Background(), k, "test", cs, tried, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if l.ChannelID != want {
		t.Fatalf("channel=%d want=%d", l.ChannelID, want)
	}
	tried[l.ChannelID] = true
	return l
}

func TestModelStickyAcrossRequestBudgetAndEndOfRound(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(26)
	// Three client requests, each with an independent channel-switch budget.
	for _, span := range [][2]int{{1, 11}, {12, 22}, {23, 26}} {
		tried := map[int]bool{}
		for id := span[0]; id <= span[1]; id++ {
			l := stickyAcquire(t, m, k, cs, tried, id == span[0], id)
			l.Failure()
		}
	}
	_, err := m.Acquire(context.Background(), k, "test", cs, map[int]bool{}, true)
	var unavailable *ModelStickyUnavailable
	if !errors.As(err, &unavailable) || unavailable.RetryAfter != 30*time.Second {
		t.Fatalf("all cooling: %v", err)
	}
	now = now.Add(30 * time.Second)
	tried := map[int]bool{}
	l := stickyAcquire(t, m, k, cs, tried, true, 1)
	l.Failure()
	// A request may not wrap after trying every channel, even after cooldown.
	for id := 2; id <= 26; id++ {
		stickyAcquire(t, m, k, cs, tried, false, id).Failure()
	}
	now = now.Add(2 * time.Minute)
	if _, err = m.Acquire(context.Background(), k, "test", cs, tried, false); err == nil {
		t.Fatal("wrapped inside same request")
	}
}

func TestModelStickyUnfinishedRoundNeverRestartsAtA(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(3)
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1).Failure()
	now = now.Add(time.Minute)
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 2).Failure()
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 3).Failure()
	// A has recovered while b/c still cool. End-of-round recovery selects a.
	l := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	l.Success()
	now = now.Add(time.Hour)
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1).Success()
}

func TestModelStickyPinNoFailbackKeyIsolationAndStaleResults(t *testing.T) {
	m := NewModelStickyManager()
	cs := stickyCandidates(3)
	k := ModelStickyKey{1, "A"}
	a := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	a.Failure()
	b := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 2)
	b.Success()
	a.Success()
	a.Failure()
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 2).Success()
	stickyAcquire(t, m, ModelStickyKey{2, "A"}, cs, map[int]bool{}, true, 1).Success()
	stickyAcquire(t, m, ModelStickyKey{1, "B"}, cs, map[int]bool{}, true, 1).Success()
	m.ResetKey(1)
	b.Success()
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1).Success()
}

func TestModelStickyConcurrentProbeCancellationAndManualPermissions(t *testing.T) {
	m := NewModelStickyManager()
	cs := stickyCandidates(3)
	k := ModelStickyKey{1, "A"}
	l := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Acquire(ctx, k, "test", cs, map[int]bool{}, true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := m.Control(k, 3); err != nil {
		t.Fatal(err)
	}
	l.Success()
	// An incompatible request cannot consume the manual target or call it.
	if _, err := m.Acquire(context.Background(), k, "test", cs[:2], map[int]bool{}, true); !errors.Is(err, ErrModelStickyRequestConflict) {
		t.Fatal(err)
	}
	if m.Snapshots()[0].PreferredChannelID != 3 {
		t.Fatal("request consumed manual target")
	}
	// Actual permission changes invalidate state through the save hook.
	m.ResetKey(k.APIKeyID)
	next := stickyAcquire(t, m, k, cs[:2], map[int]bool{}, true, 1)
	next.Success()
	if err := m.Control(k, 3); err == nil {
		t.Fatal("accepted unauthorized channel")
	}
	// A later restricted request must not inherit an inaccessible pin.
	if _, err := m.Acquire(context.Background(), k, "test", cs[1:2], map[int]bool{}, true); !errors.Is(err, ErrModelStickyRequestConflict) {
		t.Fatal(err)
	}
	if m.Snapshots()[0].ChannelID != 1 {
		t.Fatal("restricted request silently repinned")
	}
	// A real permission/configuration change explicitly invalidates old state.
	m.ResetKey(k.APIKeyID)
	stickyAcquire(t, m, k, cs[1:2], map[int]bool{}, true, 2).Success()
}

func TestModelStickyBackoffAndParallelHealthyLease(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(1)
	for _, delay := range []time.Duration{30, 60, 120, 240, 480, 600, 600} {
		l := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
		l.Failure()
		if got := m.Snapshots()[0].Cooldowns[0].Until.Sub(now); got != delay*time.Second {
			t.Fatalf("%v != %v seconds", got, delay)
		}
		now = now.Add(delay * time.Second)
	}
	l := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	l.Success()
	a := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	b := stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1)
	a.Failure()
	b.Failure()
	b.Success()
	if got := m.Snapshots()[0].Cooldowns[0].Failures; got != 1 {
		t.Fatalf("duplicate failure count %d", got)
	}
}

func TestModelStickyUntriedCoolingCandidateDeterminesRecoveryTime(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	// Existing history: a is unattempted in the new round but still cooling;
	// b was already exhausted in this round and recovers earlier.
	m.routes[k] = newModelStickyRoute()
	r := m.routes[k]
	r.attempted[2] = true
	r.health[1] = ModelStickyHealth{ChannelID: 1, Failures: 2, Until: now.Add(60 * time.Second)}
	r.health[2] = ModelStickyHealth{ChannelID: 2, Failures: 1, Until: now.Add(20 * time.Second)}
	_, err := m.Acquire(context.Background(), k, "test", cs, map[int]bool{}, true)
	var cooling *ModelStickyUnavailable
	if !errors.As(err, &cooling) || cooling.RetryAfter != time.Minute {
		t.Fatalf("incorrect next recovery: %v", err)
	}
	now = now.Add(30 * time.Second)
	if _, err = m.Acquire(context.Background(), k, "test", cs, map[int]bool{}, true); err == nil {
		t.Fatal("retried b before untried a's turn")
	}
	now = now.Add(30 * time.Second)
	stickyAcquire(t, m, k, cs, map[int]bool{}, true, 1).Success()
}
