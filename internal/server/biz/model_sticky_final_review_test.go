package biz

import (
	"context"
	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/stretchr/testify/require"
	"testing"
)

func review2Key(t *testing.T) (*APIKeyService, *ent.Client, context.Context, *ent.APIKey) {
	t.Helper()
	stickyFreshRuntime(t)
	service, client := setupTestAPIKeyService(t, xcache.Config{Mode: xcache.ModeMemory})
	t.Cleanup(func() { service.Stop(); client.Close() })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	user, err := client.User.Create().SetEmail(t.Name() + "@example.test").SetPassword("test-only").SetFirstName("test").SetLastName("test").Save(ctx)
	require.NoError(t, err)
	project, err := client.Project.Create().SetName(t.Name()).SetDescription("review").Save(ctx)
	require.NoError(t, err)
	strategy := LoadBalancerStrategyModelSticky
	profiles := objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", LoadBalanceStrategy: &strategy}}}
	key, err := client.APIKey.Create().SetName("review-key").SetKey("ah-review-only-" + t.Name()).SetType(apikey.TypeServiceAccount).SetUser(user).SetProject(project).SetProfiles(&profiles).Save(ctx)
	require.NoError(t, err)
	lease, err := ModelStickyRuntime.Acquire(ctx, ModelStickyKey{key.ID, "A"}, key.Name, stickyCandidates(2)[1:], map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	return service, client, ctx, key
}

func TestModelStickyFinalRolledBackProfileSaveMustKeepPin(t *testing.T) {
	service, client, ctx, key := review2Key(t)
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	txCtx := ent.NewContext(ent.NewTxContext(ctx, tx), tx.Client())
	strategy := LoadBalancerStrategyAdaptive
	profiles := objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", LoadBalanceStrategy: &strategy}}}
	_, saveErr := service.UpdateAPIKeyProfiles(txCtx, key.ID, profiles)
	rollbackErr := tx.Rollback()
	require.NoError(t, saveErr)
	require.NoError(t, rollbackErr)
	saved, err := client.APIKey.Get(ctx, key.ID)
	require.NoError(t, err)
	require.Equal(t, LoadBalancerStrategyModelSticky, *saved.GetActiveProfile().LoadBalanceStrategy)
	require.Len(t, ModelStickyRuntime.Snapshots(), 1, "rolled back changes must not clear routing state")
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
}

func TestModelStickyFinalManualChannelDisableMustAllowReselection(t *testing.T) {
	stickyFreshRuntime(t)
	svc, client := setupTestChannelService(t)
	t.Cleanup(func() { client.Close() })
	oldAsync := asyncReloadDisabled
	asyncReloadDisabled = true
	t.Cleanup(func() { asyncReloadDisabled = oldAsync })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	ch, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("review-a").SetBaseURL("https://example.test/v1").SetCredentials(objects.ChannelCredentials{APIKey: "test-only"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	route := ModelStickyKey{1, "A"}
	lease, err := ModelStickyRuntime.Acquire(ctx, route, "key", []ModelStickyCandidate{{ID: ch.ID, Name: "a"}, {ID: ch.ID + 1, Name: "b"}}, map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	_, err = svc.UpdateChannelStatus(ctx, ch.ID, channel.StatusDisabled)
	require.NoError(t, err)
	// The refreshed selector now correctly excludes disabled a; b remains authorized.
	next, err := ModelStickyRuntime.Acquire(ctx, route, "key", []ModelStickyCandidate{{ID: ch.ID + 1, Name: "b"}}, map[int]bool{}, true)
	require.NoError(t, err, "manual configuration change must not leave a permanent request-conflict pin")
	require.Equal(t, ch.ID+1, next.ChannelID)
	next.Abandon()
}

func TestModelStickyFinalBulkStatusChangeMustInvalidateLikeSingleStatus(t *testing.T) {
	service, _, ctx, key := review2Key(t)
	require.NoError(t, service.BulkDisableAPIKeys(ctx, []int{key.ID}))
	require.Empty(t, ModelStickyRuntime.Snapshots(), "bulk disable must fence existing leases like UpdateAPIKeyStatus")
}

func stickyFreshRuntime(t *testing.T) {
	t.Helper()
	old := ModelStickyRuntime
	ModelStickyRuntime = NewModelStickyManager()
	t.Cleanup(func() { ModelStickyRuntime = old })
}
