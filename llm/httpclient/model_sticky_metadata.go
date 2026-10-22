package httpclient

import "net/http"

import "github.com/looplj/axonhub/llm/streams"

// WithResponseHeadersAndExpectedChoices preserves both optional stream capabilities.
// Only model-sticky uses it; the legacy wrapper is deliberately unchanged.
func WithResponseHeadersAndExpectedChoices(stream streams.Stream[*StreamEvent], headers http.Header, expected int) streams.Stream[*StreamEvent] {
	if stream == nil {
		return nil
	}
	return &expectedChoicesHeadersStream{responseHeadersStream: &responseHeadersStream{Stream: stream, headers: headers.Clone()}, expected: expected}
}

type expectedChoicesHeadersStream struct {
	*responseHeadersStream
	expected int
}

func (s *expectedChoicesHeadersStream) ExpectedStreamChoices() int { return s.expected }
