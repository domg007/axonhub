package biz

import (
	"context"
	"sync"
	"testing"
	"time"
)

var reviewCandidates = []ModelStickyCandidate{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}, {ID: 3, Name: "c"}}

// Expectations describe the documented guarantee; failures reproduce gaps.
func TestReviewManualResetMustFenceOldRetry(t *testing.T) {
	m := NewModelStickyManager()
	k := ModelStickyKey{APIKeyID: 1, Model: "A"}
	old, err := m.Acquire(context.Background(), k, "key", reviewCandidates, map[int]bool{}, true)
	if err != nil {
		t.Fatal(err)
	}
	old.Failure()
	if err = m.Control(k, 0); err != nil {
		t.Fatal(err)
	}
	retry, err := m.Acquire(context.Background(), k, "key", reviewCandidates, map[int]bool{1: true}, false, old)
	if err == nil {
		retry.Success()
		t.Fatalf("old request reacquired channel %d and pinned it after manual reset; expected rejection", retry.ChannelID)
	}
}

type reviewWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *reviewWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestReviewManualControlMustFenceOldWaiter(t *testing.T) {
	m := NewModelStickyManager()
	k := ModelStickyKey{APIKeyID: 1, Model: "A"}
	old, err := m.Acquire(context.Background(), k, "key", reviewCandidates, map[int]bool{}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Abandon()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wctx := &reviewWaitContext{Context: ctx, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		l, err := m.Acquire(wctx, k, "key", reviewCandidates, map[int]bool{}, true)
		if err == nil {
			l.Success()
		}
		result <- err
	}()
	<-wctx.entered
	if err = m.Control(k, 2); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err == nil {
		t.Fatal("old waiter continued and pinned the manually selected route; expected old waiter to be fenced")
	}
}

func TestReviewRequestSpecificCandidatesCanRepinWithoutFailure(t *testing.T) {
	m := NewModelStickyManager()
	k := ModelStickyKey{APIKeyID: 1, Model: "A"}
	a, _ := m.Acquire(context.Background(), k, "key", reviewCandidates, map[int]bool{}, true)
	a.Success()
	inFlight, _ := m.Acquire(context.Background(), k, "key", reviewCandidates, map[int]bool{}, true)
	_, err := m.Acquire(context.Background(), k, "key", reviewCandidates[1:], map[int]bool{}, true)
	defer inFlight.Abandon()
	if err != ErrModelStickyRequestConflict {
		t.Fatalf("expected explicit conflict, got %v", err)
	}
	if m.Snapshots()[0].ChannelID != 1 {
		t.Fatal("request-specific restriction changed healthy pin")
	}
}
