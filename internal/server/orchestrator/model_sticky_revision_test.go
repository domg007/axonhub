package orchestrator

import (
	"context"
	"testing"

	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

type reviewMetadataStream struct {
	streams.Stream[*httpclient.StreamEvent]
}

func (*reviewMetadataStream) ExpectedStreamChoices() int { return 2 }

type reviewBadRequestStream struct {
	streams.Stream[*httpclient.StreamEvent]
}

func (*reviewBadRequestStream) Err() error {
	return &httpclient.Error{StatusCode: 400, Body: []byte(`{"error":{"message":"invalid parameter"}}`)}
}

func TestReviewStickyStreamPreservesExpectedChoices(t *testing.T) {
	stickyFreshRuntime(t)
	state := stickyTestState(2)
	if err := prepareModelSticky(context.Background(), state, &llm.Request{Model: "A"}); err != nil {
		t.Fatal(err)
	}
	original := &reviewMetadataStream{streams.SliceStream([]*httpclient.StreamEvent{})}
	result := &pipeline.Result{Stream: true, EventStream: original}
	finishModelSticky(context.Background(), state, result, nil)
	defer result.EventStream.Close()
	provider, ok := result.EventStream.(interface{ ExpectedStreamChoices() int })
	if !ok || provider.ExpectedStreamChoices() != 2 {
		t.Fatal("model-sticky wrapper dropped ExpectedStreamChoices metadata")
	}
}

func TestReviewStickyStream400DoesNotCooldown(t *testing.T) {
	stickyFreshRuntime(t)
	state := stickyTestState(2)
	if err := prepareModelSticky(context.Background(), state, &llm.Request{Model: "A"}); err != nil {
		t.Fatal(err)
	}
	result := &pipeline.Result{Stream: true, EventStream: &reviewBadRequestStream{streams.SliceStream([]*httpclient.StreamEvent{})}}
	finishModelSticky(context.Background(), state, result, nil)
	if err := result.EventStream.Close(); err != nil {
		t.Fatal(err)
	}
	if len(biz.ModelStickyRuntime.Snapshots()[0].Cooldowns) > 0 {
		t.Fatal("ordinary stream 400 was classified as channel failure and cooled down")
	}
}

func TestReviewStickyDedupDoesNotLoseFallbackModel(t *testing.T) {
	state := stickyTestState(1)
	first := state.ChannelModelsCandidates[0]
	second := *first
	second.Priority = 1
	second.Models = append([]biz.ChannelModelEntry(nil), first.Models...)
	second.Models[0].ActualModel = "backup-A"
	result := sortModelStickyCandidates([]*ChannelModelsCandidate{first, &second})
	if len(result) != 1 || len(result[0].Models) != 2 {
		t.Fatal("channel dedup dropped the lower-priority alternative model mapping")
	}
}
