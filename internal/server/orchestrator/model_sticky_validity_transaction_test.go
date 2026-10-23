package orchestrator

import (
	"testing"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/stretchr/testify/require"
)

func TestModelStickyCommittedValidityFencesOldResolutionAndLateSuccess(t *testing.T) {
	for _, kind := range []string{"model", "inheritance"} {
		for _, commit := range []bool{false, true} {
			name := kind + "/rollback"
			if commit {
				name = kind + "/commit"
			}
			t.Run(name, func(t *testing.T) {
				stickyFreshRuntime(t)
				ctx, db := setupTest(t)
				a := validityChannel(t, ctx, db, "transaction-a")
				b := validityChannel(t, ctx, db, "transaction-b")
				settings := &objects.ModelSettings{}
				if kind == "model" {
					settings.Associations = validityAssociation(a.ID)
				}
				entity := validityModel(t, ctx, db, settings)
				ms := newTestModelService(db)
				ss := newTestSystemService(db)
				if kind == "inheritance" {
					require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{DeveloperSettings: []*biz.DeveloperModelSettings{{Developer: "dev-a", Associations: validityAssociation(a.ID)}}}))
				}
				selector := NewDefaultSelector(newTestChannelServiceForChannels(db), ms, ss)
				req := &llm.Request{Model: "A", RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}
				fresh := func() *PersistenceState {
					snapshot := biz.ModelStickyRuntime.ConfigSnapshot(ctx)
					cs, err := selector.Select(ctx, req)
					require.NoError(t, err)
					return &PersistenceState{APIKey: &ent.APIKey{ID: 1, Name: "tx-key"}, OriginalModel: "A", RoutingPolicy: EffectiveRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}, ChannelModelsCandidates: cs, modelStickyConfig: snapshot}
				}
				seed := fresh()
				require.NoError(t, prepareModelSticky(ctx, seed, req))
				seed.modelSticky.release(true, false)
				staleBeforeAcquire := fresh()
				inFlight := fresh()
				require.NoError(t, prepareModelSticky(ctx, inFlight, req))
				lease := inFlight.modelSticky.lease
				tx, err := db.Tx(ctx)
				require.NoError(t, err)
				txctx := ent.NewContext(ent.NewTxContext(ctx, tx), tx.Client())
				if kind == "model" {
					_, err = ms.UpdateModel(txctx, entity.ID, &ent.UpdateModelInput{Settings: &objects.ModelSettings{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky, Associations: validityAssociation(b.ID)}})
					require.NoError(t, err)
				} else {
					require.NoError(t, ss.SetModelSettings(txctx, biz.SystemModelSettings{DeveloperSettings: []*biz.DeveloperModelSettings{{Developer: "dev-a", Associations: validityAssociation(b.ID)}}}))
				}
				require.True(t, lease.Active())
				require.Equal(t, a.ID, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
				if !commit {
					require.NoError(t, tx.Rollback())
					require.True(t, lease.Active())
					lease.Success()
					next := fresh()
					require.NoError(t, prepareModelSticky(ctx, next, req))
					require.Equal(t, a.ID, next.modelSticky.lease.ChannelID)
					next.modelSticky.release(true, false)
					return
				}
				require.NoError(t, tx.Commit())
				require.False(t, lease.Active())
				require.ErrorIs(t, prepareModelSticky(ctx, staleBeforeAcquire, req), biz.ErrModelStickySettingsChanged)
				next := fresh()
				require.NoError(t, prepareModelSticky(ctx, next, req))
				require.Equal(t, b.ID, next.modelSticky.lease.ChannelID)
				next.modelSticky.release(true, false)
				lease.Success()
				require.Equal(t, b.ID, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
				lease.Failure()
				require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
			})
		}
	}
}
