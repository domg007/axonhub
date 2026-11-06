package httpclient

import (
	"net/http"

	"github.com/looplj/axonhub/llm/streams"
)

// WithResponseHeadersAndExpectedChoices attaches response headers and an
// explicit expected-choices count to a stream. Only model-sticky uses it; the
// upstream WithResponseHeaders wrapper is deliberately unchanged.
//
// Unlike a plain decorator it keeps the optional delivery capabilities of the
// wrapped stream (DeliveryAcknowledger, DeliveryBoundary through
// responseHeadersStream.Deliver, FinalizationWaiter). Since beta12 a stream
// whose terminal delivery is never acknowledged is recorded as incomplete, so
// dropping them would mark every successful model-sticky stream as failed.
func WithResponseHeadersAndExpectedChoices(stream streams.Stream[*StreamEvent], headers http.Header, expected int) streams.Stream[*StreamEvent] {
	if stream == nil {
		return nil
	}

	result := &responseHeadersStream{
		Stream:          stream,
		headers:         headers.Clone(),
		expectedChoices: expected,
	}
	if observer, ok := stream.(streams.DeliveryAcknowledger); ok {
		result.observer = observer
	}
	if waiter, ok := stream.(streams.FinalizationWaiter); ok {
		result.waiter = waiter
	}

	return result
}
