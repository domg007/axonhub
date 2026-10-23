package orchestrator

import (
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/stretchr/testify/require"
	"testing"
)

// Adding a backup must not reset healthy routing or failure history.
func TestModelStickyAddingBackupPreservesHealthyPinAndCooldown(t *testing.T) {
	stickyFreshRuntime(t)
	ctx, db := setupTest(t)
	var ids []int
	for _, name := range []string{"overreset-a", "overreset-b", "overreset-c"} {
		ch, err := db.Channel.Create().SetType(channel.TypeOpenai).SetName(name).SetBaseURL("https://review.invalid/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
		require.NoError(t, err)
		ids = append(ids, ch.ID)
	}
	assoc := append(validityAssociation(ids[0]), validityAssociation(ids[1])...)
	m := validityModel(t, ctx, db, &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: assoc})
	ss := newTestSystemService(db)
	ms := newTestModelService(db)
	selector := NewDefaultSelector(newTestChannelServiceForChannels(db), ms, ss)
	req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
	fresh := func() *PersistenceState {
		cs, err := selector.Select(ctx, req)
		require.NoError(t, err)
		return &PersistenceState{APIKey: &ent.APIKey{ID: 1, Name: "overreset-key"}, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}, ChannelModelsCandidates: sortModelStickyCandidates(cs), modelStickyConfig: biz.ModelStickyRuntime.ConfigSnapshot(ctx)}
	}
	a := fresh()
	require.NoError(t, prepareModelSticky(ctx, a, req))
	require.Equal(t, ids[0], a.modelSticky.lease.ChannelID)
	a.modelSticky.release(false, true)
	b := fresh()
	require.NoError(t, prepareModelSticky(ctx, b, req))
	require.Equal(t, ids[1], b.modelSticky.lease.ChannelID)
	b.modelSticky.release(true, false)
	before := biz.ModelStickyRuntime.Snapshots()[0]
	require.Equal(t, ids[1], before.ChannelID)
	require.Len(t, before.Cooldowns, 1)
	// Only add a backup; existing a/b associations and selected strategy are unchanged.
	assoc = append(assoc, validityAssociation(ids[2])...)
	_, err := ms.UpdateModel(ctx, m.ID, &ent.UpdateModelInput{Settings: &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: assoc}})
	require.NoError(t, err)
	require.Equal(t, before.ChannelID, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
	require.Equal(t, before.Cooldowns, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
	next := fresh()
	require.Len(t, next.ChannelModelsCandidates, 3)
	require.NoError(t, prepareModelSticky(ctx, next, req))
	require.Equal(t, ids[1], next.modelSticky.lease.ChannelID)
	require.Equal(t, before.Cooldowns, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
	next.modelSticky.release(false, false)

}
