package orchestrator

import (
	"context"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestModelStickyFinalRequestSelectedBeforeSettingsChangeMustNotRecreateOldRoute(t *testing.T) {
	stickyFreshRuntime(t)
	ctx := context.Background()
	seed := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, seed, &llm.Request{Model: "A"}))
	seed.modelSticky.release(true, false)
	// Another request already read old Key/policy/candidates, but has not acquired a lease.
	old := stickyTestState(2)
	// Administrator changes effective policy/permissions. This is the current save hook.
	biz.ModelStickyRuntime.ResetKey(old.APIKey.ID)
	require.Empty(t, biz.ModelStickyRuntime.Snapshots())
	err := prepareModelSticky(ctx, old, &llm.Request{Model: "A"})
	if err == nil {
		err = finishModelSticky(ctx, old, &pipeline.Result{Response: &httpclient.Response{}}, nil)
	}
	require.Error(t, err, "old pre-acquisition request must not resurrect state after configuration invalidation")
	require.Empty(t, biz.ModelStickyRuntime.Snapshots())
}

func TestModelStickyFinalManualTargetMustSurviveNonFaultRequest(t *testing.T) {
	stickyFreshRuntime(t)
	ctx := context.Background()
	seed := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, seed, &llm.Request{Model: "A"}))
	seed.modelSticky.release(true, false)
	require.NoError(t, biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: 1, Model: "A"}, 2))
	bad := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, bad, &llm.Request{Model: "A"}))
	require.Equal(t, 2, bad.modelSticky.lease.ChannelID)
	bad.CurrentCandidate = bad.ChannelModelsCandidates[0]
	require.Error(t, finishModelSticky(ctx, bad, nil, &httpclient.Error{StatusCode: 400}))
	require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns, "parameter error is not a channel failure")
	next := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, next, &llm.Request{Model: "A"}))
	defer next.modelSticky.release(false, false)
	require.Equal(t, 2, next.modelSticky.lease.ChannelID, "manual target should not disappear because of an ordinary invalid request")
}
