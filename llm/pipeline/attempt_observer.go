package pipeline

import "context"

// AttemptObserver receives every attempt result, including the last exhausted
// attempt for which CanRetry/NextChannel will never be called. It must not
// interpret successful stream setup as successful stream completion.
type AttemptObserver interface {
	ObserveAttempt(context.Context, error)
}

func (p *pipeline) observeAttempt(ctx context.Context, err error) {
	if observer, ok := p.Outbound.(AttemptObserver); ok {
		observer.ObserveAttempt(ctx, err)
	}
}
