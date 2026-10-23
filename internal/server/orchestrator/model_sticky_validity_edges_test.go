package orchestrator

import (
	"context"
	"strings"
	"testing"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/stretchr/testify/require"
)

func validityChannel(t *testing.T, ctx context.Context, db *ent.Client, name string) *ent.Channel {
	t.Helper()
	ch, err := db.Channel.Create().SetType(channel.TypeOpenai).SetName(name).SetBaseURL("https://review.invalid/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	return ch
}

func TestModelStickyDisabledDeletedFallbackIsScopedToEffectiveStrategy(t *testing.T) {
	for _, strategy := range []string{biz.LoadBalancerStrategyModelSticky, biz.LoadBalancerStrategyAdaptive} {
		for _, operation := range []string{"disable", "archive", "delete"} {
			t.Run(strategy+"/"+operation, func(t *testing.T) {
				stickyFreshRuntime(t)
				ctx, db := setupTest(t)
				ch := validityChannel(t, ctx, db, "fallback-a")
				entity := validityModel(t, ctx, db, &objects.ModelSettings{Associations: validityAssociation(ch.ID)})
				// Explicit Key policy wins over the configured model's sticky policy.
				key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled, Profiles: &objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", LoadBalanceStrategy: &strategy}}}}
				ctx = contexts.WithAPIKey(ctx, key)
				ms := newTestModelService(db)
				ss := newTestSystemService(db)
				require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{FallbackToChannelsOnModelNotFound: true}))
				switch operation {
				case "disable":
					_, err := ms.UpdateModelStatus(ctx, entity.ID, model.StatusDisabled)
					require.NoError(t, err)
				case "archive":
					require.NoError(t, ms.BulkArchiveModels(ctx, []int{entity.ID}))
				case "delete":
					require.NoError(t, ms.DeleteModel(ctx, entity.ID))
				}
				selector := NewDefaultSelector(newTestChannelServiceForChannels(db), ms, ss)
				req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
				cs, err := selector.Select(ctx, req)
				if strategy == biz.LoadBalancerStrategyModelSticky {
					require.ErrorIs(t, err, biz.ErrInvalidModel)
					require.Empty(t, cs)
				} else {
					require.NoError(t, err)
					require.Len(t, cs, 1)
					require.Nil(t, cs[0].ModelRoutingPolicy)
				}
				// Selecting the channel directly is neither configured-model routing nor
				// sticky failover, even though its model has the same name as disabled A.
				cs, err = NewSpecifiedChannelSelector(selector.ChannelService, objects.GUID{Type: "Channel", ID: ch.ID}).Select(ctx, req)
				require.NoError(t, err)
				require.Len(t, cs, 1)
				require.Nil(t, cs[0].ModelRoutingPolicy)
				require.Empty(t, biz.ModelStickyRuntime.Snapshots())
			})
		}
	}
}

func TestModelStickyWhenEmptyPreservesPinButRemovedAssociationRetiresIt(t *testing.T) {
	stickyFreshRuntime(t)
	ctx, db := setupTest(t)
	ch := validityChannel(t, ctx, db, "when-a")
	assoc := validityAssociation(ch.ID)
	assoc[0].When = &objects.ModelAssociationWhen{Enabled: true, Condition: &objects.Condition{Logic: "and", Conditions: []objects.Condition{{Field: "prompt_tokens", Operator: "lt", Value: 100}}}}
	entity := validityModel(t, ctx, db, &objects.ModelSettings{Associations: assoc})
	ms := newTestModelService(db)
	ss := newTestSystemService(db)
	selector := NewDefaultSelector(newTestChannelServiceForChannels(db), ms, ss)
	key := &ent.APIKey{ID: 1, Name: "when-key", Status: apikey.StatusEnabled}
	selectState := func(req *llm.Request) (context.Context, *PersistenceState) {
		requestCtx := context.WithValue(ctx, modelStickyResolutionContextKey{}, &modelStickyResolutionHolder{})
		snapshot := biz.ModelStickyRuntime.ConfigSnapshot(requestCtx)
		cs, err := selector.Select(requestCtx, req)
		require.NoError(t, err)
		return requestCtx, &PersistenceState{APIKey: key, OriginalModel: "A", RoutingPolicy: deriveRequestRoutingPolicy(requestCtx, ss.RetryPolicyOrDefault(ctx), key, cs), ChannelModelsCandidates: cs, modelStickyConfig: snapshot}
	}
	small := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion, Messages: []llm.Message{{Role: "user", Content: llm.MessageContent{Content: new("hello")}}}}
	requestCtx, state := selectState(small)
	require.NoError(t, prepareModelSticky(requestCtx, state, small))
	state.modelSticky.release(true, false)
	large := *small
	large.Messages = []llm.Message{{Role: "user", Content: llm.MessageContent{Content: new(strings.Repeat("0123456789", 500))}}}
	requestCtx, state = selectState(&large)
	require.Empty(t, state.ChannelModelsCandidates)
	require.ErrorIs(t, prepareModelSticky(requestCtx, state, &large), biz.ErrModelStickyRequestConflict)
	require.Equal(t, ch.ID, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
	// No association remains in this configured A: retire the target, even though
	// the request also has zero final candidates. Do not keep returning stale 409.
	_, err := ms.UpdateModel(ctx, entity.ID, &ent.UpdateModelInput{Settings: &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: []*objects.ModelAssociation{}, DisableDeveloperSettingsInheritance: true}})
	require.NoError(t, err)
	requestCtx, state = selectState(small)
	err = prepareModelSticky(requestCtx, state, small)
	require.Error(t, err)
	require.NotErrorIs(t, err, biz.ErrModelStickyRequestConflict)
	require.Zero(t, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
	require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
}

func TestModelStickyPermissionRestrictionRelaysAndExpansionKeepsPin(t *testing.T) {
	for _, scope := range []string{"key", "project"} {
		t.Run(scope, func(t *testing.T) {
			stickyFreshRuntime(t)
			ctx, db := setupTest(t)
			a := validityChannel(t, ctx, db, "permission-a")
			b := validityChannel(t, ctx, db, "permission-b")
			validityModel(t, ctx, db, &objects.ModelSettings{Associations: append(validityAssociation(a.ID), validityAssociation(b.ID)...)})
			selector := NewDefaultSelector(newTestChannelServiceForChannels(db), newTestModelService(db), newTestSystemService(db))
			key := &ent.APIKey{ID: 1, Name: "permission-key", ProjectID: 1, Status: apikey.StatusEnabled}
			project := &ent.Project{ID: 1}
			key.Edges.Project = project
			req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
			fresh := func() *PersistenceState {
				var decorated CandidateSelector = selector
				ids := []int(nil)
				if scope == "key" {
					if p := key.GetActiveProfile(); p != nil {
						ids = p.ChannelIDs
					}
				} else {
					if p := project.GetActiveProfile(); p != nil {
						ids = p.ChannelIDs
					}
				}
				if len(ids) > 0 {
					decorated = WithSelectedChannelsSelector(decorated, ids)
				}
				cs, err := decorated.Select(ctx, req)
				require.NoError(t, err)
				return &PersistenceState{APIKey: key, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}, ChannelModelsCandidates: cs, modelStickyConfig: biz.ModelStickyRuntime.ConfigSnapshot(ctx)}
			}
			state := fresh()
			require.NoError(t, prepareModelSticky(ctx, state, req))
			state.modelSticky.release(true, false)
			setPermissions := func(ids []int) {
				if scope == "key" {
					key.Profiles = &objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", ChannelIDs: ids}}}
					biz.ModelStickyRuntime.PublishKey(key)
				} else {
					project.Profiles = &objects.ProjectProfiles{ActiveProfile: "main", Profiles: []objects.ProjectProfile{{Name: "main", ChannelIDs: ids}}}
					biz.ModelStickyRuntime.PublishProject(project)
				}
			}
			setPermissions([]int{b.ID})
			state = fresh()
			require.NoError(t, prepareModelSticky(ctx, state, req))
			require.Equal(t, b.ID, state.modelSticky.lease.ChannelID)
			state.modelSticky.release(true, false)
			setPermissions([]int{a.ID, b.ID})
			state = fresh()
			require.NoError(t, prepareModelSticky(ctx, state, req))
			require.Equal(t, b.ID, state.modelSticky.lease.ChannelID)
			state.modelSticky.release(true, false)
			require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
		})
	}
}
