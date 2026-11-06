package orchestrator

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

// stickyDeliverySource exposes every optional stream capability that the
// transport and accounting layers rely on since beta12.
type stickyDeliverySource struct {
	streams.Stream[*httpclient.StreamEvent]

	acknowledged bool
	delivered    int
	waited       bool
}

func (s *stickyDeliverySource) AcknowledgeDelivery()       { s.acknowledged = true }
func (s *stickyDeliverySource) DeliveryAcknowledged() bool { return s.acknowledged }
func (s *stickyDeliverySource) WaitForFinalization()       { s.waited = true }
func (s *stickyDeliverySource) ExpectedStreamChoices() int { return 2 }

func (s *stickyDeliverySource) Deliver(_ context.Context, write func() (bool, error)) error {
	s.delivered++
	terminal, err := write()
	if err == nil && terminal {
		s.acknowledged = true
	}
	return err
}

// lifecycleDeliverySource delegates delivery to a real upstream streamLifecycle,
// the same way InboundPersistentStream does.
type lifecycleDeliverySource struct {
	streams.Stream[*httpclient.StreamEvent]

	lifecycle *streamLifecycle
}

func (s *lifecycleDeliverySource) AcknowledgeDelivery() { s.lifecycle.AcknowledgeDelivery() }
func (s *lifecycleDeliverySource) DeliveryAcknowledged() bool {
	return s.lifecycle.DeliveryAcknowledged()
}
func (s *lifecycleDeliverySource) WaitForFinalization() { s.lifecycle.WaitForFinalization() }

func (s *lifecycleDeliverySource) Deliver(ctx context.Context, write func() (bool, error)) error {
	return s.lifecycle.Deliver(ctx, write)
}

func (s *lifecycleDeliverySource) Close() error {
	err := s.Stream.Close()
	s.lifecycle.Close()
	return err
}

// The model-sticky wrapper must not hide the delivery capabilities of the stream
// it wraps. Before the beta12 merge fix the wrapper only forwarded Next/Close, so
// the terminal acknowledgement never reached the source and every successful
// model-sticky stream was recorded as incomplete.
func TestModelStickyStreamKeepsDeliveryCapabilities(t *testing.T) {
	for _, headers := range []http.Header{nil, {"X-Request-Id": []string{"id"}}} {
		stickyFreshRuntime(t)
		s := stickyTestState(1)
		require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
		require.NotNil(t, s.modelSticky.lease)

		source := &stickyDeliverySource{Stream: streams.SliceStream([]*httpclient.StreamEvent{{Data: []byte(`{}`)}})}
		var original streams.Stream[*httpclient.StreamEvent] = source
		if headers != nil {
			original = httpclient.WithResponseHeaders(source, headers)
		}
		result := &pipeline.Result{Stream: true, EventStream: original}
		require.NoError(t, finishModelSticky(context.Background(), s, result, nil))

		boundary, ok := result.EventStream.(streams.DeliveryBoundary)
		require.True(t, ok)
		require.NoError(t, boundary.Deliver(context.Background(), func() (bool, error) { return true, nil }))
		require.Equal(t, 1, source.delivered, "Deliver must reach the source boundary")

		observer, ok := result.EventStream.(streams.DeliveryAcknowledger)
		require.True(t, ok)
		require.True(t, observer.DeliveryAcknowledged(), "acknowledgement must be visible through the wrapper")

		waiter, ok := result.EventStream.(streams.FinalizationWaiter)
		require.True(t, ok)
		waiter.WaitForFinalization()
		require.True(t, source.waited, "finalization wait must reach the source")

		choices, ok := result.EventStream.(interface{ ExpectedStreamChoices() int })
		require.True(t, ok)
		require.Equal(t, 2, choices.ExpectedStreamChoices())
		require.Equal(t, headers.Get("X-Request-Id"), httpclient.GetResponseHeaders(result.EventStream).Get("X-Request-Id"))

		// Close still runs through the model-sticky wrapper and releases the lease.
		require.NoError(t, result.EventStream.Close())
		require.Nil(t, s.modelSticky.lease)
	}
}

// Same contract against the real upstream stream lifecycle: a terminal write
// through the outermost wrapper must mark the lifecycle as acknowledged, which
// InboundPersistentStream.Close uses to decide whether the stream completed.
func TestModelStickyStreamAcknowledgesUpstreamLifecycle(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(1)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))

	owner, executionCtx := newStreamLifecycle(context.Background())
	source := &lifecycleDeliverySource{
		Stream:    streams.SliceStream([]*httpclient.StreamEvent{{Data: []byte(`{}`)}}),
		lifecycle: owner,
	}
	result := &pipeline.Result{Stream: true, EventStream: httpclient.WithResponseHeaders(source, http.Header{"X-Request-Id": []string{"id"}})}
	require.NoError(t, finishModelSticky(executionCtx, s, result, nil))

	// Mirrors api.writeStreamDelivery for the terminal event.
	boundary, ok := result.EventStream.(streams.DeliveryBoundary)
	require.True(t, ok)
	require.NoError(t, boundary.Deliver(context.Background(), func() (bool, error) { return true, nil }))
	require.True(t, owner.DeliveryAcknowledged())

	require.NoError(t, result.EventStream.Close())
	result.EventStream.(streams.FinalizationWaiter).WaitForFinalization()
	require.True(t, owner.DeliveryAcknowledged())
	require.Nil(t, s.modelSticky.lease)
}
