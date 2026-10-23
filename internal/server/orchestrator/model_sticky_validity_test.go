package orchestrator

import (
	"context"
	"testing"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/stretchr/testify/require"
)

// Regression coverage uses real selector and model/system management services.
func validityAssociation(id int) []*objects.ModelAssociation {
	return []*objects.ModelAssociation{{Type: "channel_model", ChannelModel: &objects.ChannelModelAssociation{ChannelID: id, ModelID: "A"}}}
}
func validityModel(t *testing.T, ctx context.Context, db *ent.Client, settings *objects.ModelSettings) *ent.Model {
	t.Helper()
	settings.LoadBalancerStrategy = biz.LoadBalancerStrategyModelSticky
	m, err := db.Model.Create().SetDeveloper("dev-a").SetModelID("A").SetType(model.TypeChat).SetName("A").SetIcon("openai").SetGroup("review").SetModelCard(&objects.ModelCard{}).SetSettings(settings).SetStatus(model.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	return m
}
func TestModelStickyResolvedConfigurationRelaysWithinModel(t *testing.T) {
	for _, mode := range []string{"developer-settings", "model-developer", "delete-recreate", "settings-control"} {
		t.Run(mode, func(t *testing.T) {
			stickyFreshRuntime(t)
			ctx, db := setupTest(t)
			makeChannel := func(name string) *ent.Channel {
				ch, err := db.Channel.Create().SetType(channel.TypeOpenai).SetName(name).SetBaseURL("https://review.invalid/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
				require.NoError(t, err)
				return ch
			}
			a, b := makeChannel("review-a"), makeChannel("review-b")
			ss := newTestSystemService(db)
			ms := newTestModelService(db)
			settings := &objects.ModelSettings{}
			if mode == "delete-recreate" || mode == "settings-control" {
				settings.Associations = validityAssociation(a.ID)
			}
			m := validityModel(t, ctx, db, settings)
			require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{DeveloperSettings: []*biz.DeveloperModelSettings{{Developer: "dev-a", Associations: validityAssociation(a.ID)}, {Developer: "dev-b", Associations: validityAssociation(b.ID)}}}))
			if len(settings.Associations) > 0 {
				require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{}))
			}
			selector := NewDefaultSelector(newTestChannelServiceForChannels(db), ms, ss)
			req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
			selectState := func() *PersistenceState {
				candidates, err := selector.Select(ctx, req)
				require.NoError(t, err)
				require.Len(t, candidates, 1)
				return &PersistenceState{APIKey: &ent.APIKey{ID: 1, Name: "review-key"}, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}, ChannelModelsCandidates: candidates, modelStickyConfig: biz.ModelStickyRuntime.ConfigSnapshot(ctx)}
			}
			seed := selectState()
			require.Equal(t, a.ID, seed.ChannelModelsCandidates[0].Channel.ID)
			require.NoError(t, prepareModelSticky(ctx, seed, req))
			seed.modelSticky.release(true, false)
			switch mode {
			case "developer-settings":
				require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{DeveloperSettings: []*biz.DeveloperModelSettings{{Developer: "dev-a", Associations: validityAssociation(b.ID)}}}))
			case "model-developer":
				dev := "dev-b"
				_, err := ms.UpdateModel(ctx, m.ID, &ent.UpdateModelInput{Developer: &dev})
				require.NoError(t, err)
			case "delete-recreate":
				require.NoError(t, ms.DeleteModel(ctx, m.ID))
				validityModel(t, ctx, db, &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: validityAssociation(b.ID)})
			case "settings-control":
				_, err := ms.UpdateModel(ctx, m.ID, &ent.UpdateModelInput{Settings: &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: validityAssociation(b.ID)}})
				require.NoError(t, err)
			}
			next := selectState()
			require.Equal(t, b.ID, next.ChannelModelsCandidates[0].Channel.ID, "selector sees changed configuration")
			err := prepareModelSticky(ctx, next, req)
			require.NoError(t, err)
			require.Equal(t, b.ID, next.modelSticky.lease.ChannelID)
			next.modelSticky.release(true, false)
			require.Equal(t, b.ID, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)

		})
	}
}

func TestModelStickyResolvedLifecycleFencesOldLease(t *testing.T) {
	for _, mode := range []string{"status-disable", "bulk-disable", "bulk-archive", "delete", "bulk-delete", "update-control"} {
		t.Run(mode, func(t *testing.T) {
			stickyFreshRuntime(t)
			ctx, db := setupTest(t)
			ch, err := db.Channel.Create().SetType(channel.TypeOpenai).SetName("lifecycle-a").SetBaseURL("https://review.invalid/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
			require.NoError(t, err)
			m := validityModel(t, ctx, db, &objects.ModelSettings{Associations: validityAssociation(ch.ID)})
			selector := NewDefaultSelector(newTestChannelServiceForChannels(db), newTestModelService(db), newTestSystemService(db))
			req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
			cs, err := selector.Select(ctx, req)
			require.NoError(t, err)
			state := &PersistenceState{APIKey: &ent.APIKey{ID: 1, Name: "lifecycle-key"}, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}, ChannelModelsCandidates: cs, modelStickyConfig: biz.ModelStickyRuntime.ConfigSnapshot(ctx)}
			require.NoError(t, prepareModelSticky(ctx, state, req))
			lease := state.modelSticky.lease
			ms := newTestModelService(db)
			switch mode {
			case "status-disable":
				_, err := ms.UpdateModelStatus(ctx, m.ID, model.StatusDisabled)
				require.NoError(t, err)
			case "bulk-disable":
				require.NoError(t, ms.BulkDisableModels(ctx, []int{m.ID}))
			case "bulk-archive":
				require.NoError(t, ms.BulkArchiveModels(ctx, []int{m.ID}))
			case "delete":
				require.NoError(t, ms.DeleteModel(ctx, m.ID))
			case "bulk-delete":
				require.NoError(t, ms.BulkDeleteModels(ctx, []int{m.ID}))
			case "update-control":
				status := model.StatusDisabled
				_, err := ms.UpdateModel(ctx, m.ID, &ent.UpdateModelInput{Status: &status})
				require.NoError(t, err)
			}
			require.False(t, lease.Active())
			lease.Success()
			require.Empty(t, biz.ModelStickyRuntime.Snapshots())

		})
	}
}
