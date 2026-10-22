package biz

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type stickyAcquireResult struct {
	lease *ModelStickyLease
	err   error
}

func stickyAsyncAcquire(m *ModelStickyManager, ctx context.Context, key ModelStickyKey, cs []ModelStickyCandidate) <-chan stickyAcquireResult {
	result := make(chan stickyAcquireResult, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				result <- stickyAcquireResult{err: fmt.Errorf("probe waiter panic: %v", r)}
			}
		}()
		lease, err := m.Acquire(ctx, key, "test", cs, map[int]bool{}, true)
		result <- stickyAcquireResult{lease, err}
	}()
	return result
}

func TestModelStickyConcurrentColdStartAndHealthyRequests(t *testing.T) {
	m := NewModelStickyManager()
	key := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	first := stickyAcquire(t, m, key, cs, map[int]bool{}, true, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiter := stickyAsyncAcquire(m, ctx, key, cs)
	select {
	case got := <-waiter:
		t.Fatalf("concurrent cold start escaped probe: %v", got)
	case <-time.After(20 * time.Millisecond):
	}
	first.Success()
	select {
	case got := <-waiter:
		if got.err != nil || got.lease.ChannelID != 1 {
			t.Fatalf("waiter = %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Multiple healthy leases need not wait for earlier requests to complete.
	one := stickyAsyncAcquire(m, ctx, key, cs)
	two := stickyAsyncAcquire(m, ctx, key, cs)
	for _, ch := range []<-chan stickyAcquireResult{one, two} {
		select {
		case got := <-ch:
			if got.err != nil || got.lease.ChannelID != 1 {
				t.Fatalf("healthy = %+v", got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestModelStickyResetCannotBeUndoneByOldWaitersOrRetries(t *testing.T) {
	m := NewModelStickyManager()
	key := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	first := stickyAcquire(t, m, key, cs, map[int]bool{}, true, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiter := stickyAsyncAcquire(m, ctx, key, cs)
	select {
	case <-waiter:
		t.Fatal("probe waiter escaped")
	case <-time.After(20 * time.Millisecond):
	}
	m.ResetKey(1)
	select {
	case got := <-waiter:
		if got.err == nil {
			t.Fatal("old waiter recreated routing state")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	first.Success()
	if len(m.Snapshots()) != 0 {
		t.Fatal("late success recreated state")
	}
	if _, err := m.Acquire(ctx, key, "test", cs, map[int]bool{1: true}, false, first); err == nil {
		t.Fatal("old request reentered after reset")
	}
	stickyAcquire(t, m, key, cs, map[int]bool{}, true, 1).Success()
}

func TestModelStickyExistingPinCannotCreateParallelFallback(t *testing.T) {
	m := NewModelStickyManager()
	key := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	stickyAcquire(t, m, key, cs, map[int]bool{}, true, 1).Success()
	if _, err := m.Acquire(context.Background(), key, "test", cs, map[int]bool{1: true}, false); err == nil {
		t.Fatal("started another channel beside the healthy pin")
	}
	row := m.Snapshots()[0]
	if row.ChannelID != 1 || row.Probing {
		t.Fatal("healthy pin was changed")
	}
}
