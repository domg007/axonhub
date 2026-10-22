package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/stretchr/testify/require"
)

type stickyTestInbound struct{ transformer.Inbound }

func (*stickyTestInbound) TransformRequest(context.Context, *httpclient.Request) (*llm.Request, error) {
	return &llm.Request{Model: "A"}, nil
}
func (*stickyTestInbound) TransformResponse(context.Context, *llm.Response) (*httpclient.Response, error) {
	return &httpclient.Response{StatusCode: 200, Body: []byte(`{"ok":true}`)}, nil
}

type stickyTestOutbound struct {
	transformer.Outbound
	id int
}

func (*stickyTestOutbound) APIFormat() llm.APIFormat { return llm.APIFormatOpenAIChatCompletion }
func (s *stickyTestOutbound) TransformRequest(context.Context, *llm.Request) (*httpclient.Request, error) {
	return &httpclient.Request{URL: strconv.Itoa(s.id)}, nil
}
func (*stickyTestOutbound) TransformResponse(context.Context, *httpclient.Response) (*llm.Response, error) {
	return &llm.Response{ID: "ok"}, nil
}
func (*stickyTestOutbound) TransformError(_ context.Context, err *httpclient.Error) *llm.ResponseError {
	return &llm.ResponseError{StatusCode: err.StatusCode, Detail: llm.ErrorDetail{Message: "test upstream failure"}}
}

type stickyTestExecutor struct {
	attempts  []int
	successID int
}

func (s *stickyTestExecutor) Do(_ context.Context, req *httpclient.Request) (*httpclient.Response, error) {
	id, _ := strconv.Atoi(req.URL)
	s.attempts = append(s.attempts, id)
	if id == s.successID {
		return &httpclient.Response{StatusCode: 200}, nil
	}
	return nil, &httpclient.Error{StatusCode: 429, Body: []byte(`{"error":{"message":"limited"}}`), Headers: http.Header{"Retry-After": []string{"3600"}}}
}
func (*stickyTestExecutor) DoStream(context.Context, *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	panic("unexpected network stream")
}

func stickyTestState(n int) *PersistenceState {
	s := &PersistenceState{modelStickyConfig: biz.ModelStickyRuntime.ConfigSnapshot(context.Background()), APIKey: &ent.APIKey{ID: 1, Name: "test-key"}, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}}
	for id := 1; id <= n; id++ {
		s.ChannelModelsCandidates = append(s.ChannelModelsCandidates, &ChannelModelsCandidate{
			Channel:   &biz.Channel{Channel: &ent.Channel{ID: id, Name: fmt.Sprint(id)}, Outbound: &stickyTestOutbound{id: id}},
			APIFormat: string(llm.APIFormatOpenAIChatCompletion), Models: []biz.ChannelModelEntry{{RequestModel: "A", ActualModel: "actual-A"}}})
	}
	return s
}
func stickyTestProcess(t *testing.T, s *PersistenceState, e *stickyTestExecutor, cross, same int) (*pipeline.Result, error) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, prepareModelSticky(ctx, s, &llm.Request{Model: "A"}))
	out := &PersistentOutboundTransformer{state: s}
	p := pipeline.NewFactory(e).Pipeline(&stickyTestInbound{}, out, pipeline.WithRetry(cross, same, 0))
	result, err := p.Process(ctx, &httpclient.Request{})
	finishModelSticky(ctx, s, result, err)
	return result, err
}

func TestModelStickyPipeline429BudgetContinuationAndFinalExhaustion(t *testing.T) {
	stickyFreshRuntime(t)
	executor := &stickyTestExecutor{}
	_, err := stickyTestProcess(t, stickyTestState(26), executor, 10, 2)
	require.Error(t, err)
	require.Len(t, executor.attempts, 33)
	for id := 1; id <= 11; id++ {
		require.Equal(t, []int{id, id, id}, executor.attempts[(id-1)*3:id*3])
	}
	require.Len(t, biz.ModelStickyRuntime.Snapshots()[0].Attempted, 11, "last exhausted channel must be recorded")
	executor.attempts = nil
	_, err = stickyTestProcess(t, stickyTestState(26), executor, 10, 0)
	require.Error(t, err)
	require.Equal(t, []int{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}, executor.attempts)
	executor.attempts = nil
	_, err = stickyTestProcess(t, stickyTestState(26), executor, 10, 0)
	require.Error(t, err)
	require.Equal(t, []int{23, 24, 25, 26}, executor.attempts)
	s := stickyTestState(26)
	require.Error(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}), "no early restart while all channels cool")
}

func TestModelStickySuccessPinsAndOtherStrategiesKeep429Behavior(t *testing.T) {
	stickyFreshRuntime(t)
	executor := &stickyTestExecutor{successID: 2}
	result, err := stickyTestProcess(t, stickyTestState(3), executor, 2, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []int{1, 1, 2}, executor.attempts)
	executor.attempts = nil
	_, err = stickyTestProcess(t, stickyTestState(3), executor, 2, 1)
	require.NoError(t, err)
	require.Equal(t, []int{2}, executor.attempts)
	s := stickyTestState(1)
	s.CurrentCandidate = s.ChannelModelsCandidates[0]
	out := &PersistentOutboundTransformer{state: s}
	require.True(t, out.CanRetry(&httpclient.Error{StatusCode: 429}))
	s.RoutingPolicy.LoadBalancerStrategy = biz.LoadBalancerStrategyAdaptive
	require.False(t, out.CanRetry(&httpclient.Error{StatusCode: 429}))
}

func TestModelStickyOrderingDeduplicatesEqualWeights(t *testing.T) {
	s := stickyTestState(5)
	cs := s.ChannelModelsCandidates
	cs[0].Channel.OrderingWeight = 1
	cs[1].Channel.OrderingWeight = 5
	cs[2].Channel.OrderingWeight = 5
	cs[3].Channel.OrderingWeight = 5
	cs[4].Priority = -1
	got := sortModelStickyCandidates([]*ChannelModelsCandidate{cs[3], cs[2], cs[1], cs[0], cs[3], cs[4]})
	ids := []int{}
	for _, c := range got {
		ids = append(ids, c.Channel.ID)
	}
	require.Equal(t, []int{5, 2, 3, 4, 1}, ids)
}

func TestModelStickyMetricsAndNonRetryableErrors(t *testing.T) {
	s := stickyTestState(1)
	s.CurrentCandidate = s.ChannelModelsCandidates[0]
	out := &PersistentOutboundTransformer{state: s}
	m := &performanceRecording{outbound: out}
	_, err := m.OnOutboundRawRequest(context.Background(), &httpclient.Request{})
	require.NoError(t, err)
	require.True(t, s.Perf.SkipAutoDisable)
	require.False(t, modelStickyRetryable(context.Background(), s, &httpclient.Error{StatusCode: 400}))
	require.False(t, modelStickyRetryable(context.Background(), s, context.Canceled))
	require.True(t, modelStickyRetryable(context.Background(), s, pipeline.ErrNonStreamResponseTimeout))
}

func TestModelStickyStreamRequiresValidationAndReleasesOnClose(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			stickyFreshRuntime(t)
			s := stickyTestState(2)
			require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
			s.Perf = &biz.PerformanceRecord{Success: success, RequestCompleted: true}
			if !success {
				s.OutboundStreamTerminal = streamTerminalFailed
			}
			result := &pipeline.Result{Stream: true, EventStream: streams.SliceStream([]*httpclient.StreamEvent{})}
			finishModelSticky(context.Background(), s, result, nil)
			require.True(t, biz.ModelStickyRuntime.Snapshots()[0].Probing, "SSE setup is not success")
			require.NoError(t, result.EventStream.Close())
			require.NoError(t, result.EventStream.Close())
			row := biz.ModelStickyRuntime.Snapshots()[0]
			require.False(t, row.Probing)
			if success {
				require.Equal(t, 1, row.ChannelID)
			} else {
				require.Equal(t, []int{1}, row.Attempted)
			}
		})
	}
}

func TestModelStickyContextCancellationReleasesProbe(t *testing.T) {
	stickyFreshRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, s, &llm.Request{Model: "A"}))
	cancel()
	require.Eventually(t, func() bool { return !biz.ModelStickyRuntime.Snapshots()[0].Probing }, time.Second, time.Millisecond)
	finishModelSticky(ctx, s, nil, context.Canceled)
	require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Attempted)
	require.True(t, errors.Is(ctx.Err(), context.Canceled))
}

func TestModelStickyExplicitModelErrorsNotArbitrary404(t *testing.T) {
	s := stickyTestState(1)
	s.CurrentCandidate = s.ChannelModelsCandidates[0]
	for _, test := range []struct {
		body string
		want bool
	}{
		{`{"error":{"code":"model_not_found"}}`, true},
		{`{"error":{"type":"not_found_error","message":"model: claude-test"}}`, true},
		{`{"error":{"type":"not_found_error","message":"file: missing"}}`, false},
		{`{"error":{"code":"context_length_exceeded"}}`, false},
		{`{"error":{"code":"content_policy_violation"}}`, false},
	} {
		require.Equal(t, test.want, modelStickyRetryable(context.Background(), s, &httpclient.Error{StatusCode: 404, Body: []byte(test.body)}), test.body)
	}
}

func TestModelStickyStreamPreservesForwardedHeaders(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(1)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
	stream := httpclient.WithResponseHeaders(streams.SliceStream([]*httpclient.StreamEvent{}), http.Header{"X-Request-Id": []string{"test-id"}})
	result := &pipeline.Result{Stream: true, EventStream: stream}
	finishModelSticky(context.Background(), s, result, nil)
	require.Equal(t, "test-id", httpclient.GetResponseHeaders(result.EventStream).Get("X-Request-Id"))
	require.NoError(t, result.EventStream.Close())
}

func stickyFreshRuntime(t *testing.T) {
	t.Helper()
	previous := biz.ModelStickyRuntime
	biz.ModelStickyRuntime = biz.NewModelStickyManager()
	t.Cleanup(func() { biz.ModelStickyRuntime.ResetAll(); biz.ModelStickyRuntime = previous })
}
