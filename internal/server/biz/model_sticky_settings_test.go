package biz

import (
	"context"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/objects"
	"testing"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/stretchr/testify/require"
)

func TestModelStickySystemStrategySaveInvalidatesWithoutTraffic(t *testing.T) {
	stickyFreshRuntime(t)
	client := enttest.NewEntClient(t, "sqlite3", "file:sticky-settings?mode=memory&_fk=1")
	defer client.Close()
	service := NewSystemService(SystemServiceParams{Ent: client, CacheConfig: xcache.Config{Mode: xcache.ModeMemory}})
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	policy := &RetryPolicy{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}
	require.NoError(t, service.SetRetryPolicy(ctx, policy))
	require.Equal(t, LoadBalancerStrategyModelSticky, service.RetryPolicyOrDefault(ctx).LoadBalancerStrategy)
	key := ModelStickyKey{1, "A"}
	old, err := ModelStickyRuntime.Acquire(ctx, key, "test", stickyCandidates(2), map[int]bool{}, true)
	require.NoError(t, err)
	old.Success()
	require.Len(t, ModelStickyRuntime.Snapshots(), 1)
	require.NoError(t, service.SetRetryPolicy(ctx, &RetryPolicy{LoadBalancerStrategy: LoadBalancerStrategyAdaptive}))
	require.Empty(t, ModelStickyRuntime.Snapshots())
	// No model requests occur between switching away and switching back.
	require.NoError(t, service.SetRetryPolicy(ctx, &RetryPolicy{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}))
	old.Success()
	old.Failure()
	require.Empty(t, ModelStickyRuntime.Snapshots())
}

func TestModelStickyRealKeySavesKeepRenameButClearStrategyChange(t *testing.T) {
	stickyFreshRuntime(t)
	service, client := setupTestAPIKeyService(t, xcache.Config{Mode: xcache.ModeMemory})
	defer client.Close()
	defer service.Stop()
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	user, err := client.User.Create().SetEmail("sticky-save@example.test").SetPassword("test-only").SetFirstName("test").SetLastName("test").Save(ctx)
	require.NoError(t, err)
	project, err := client.Project.Create().SetName("sticky-save").SetDescription("test").Save(ctx)
	require.NoError(t, err)
	strategy := LoadBalancerStrategyModelSticky
	profiles := objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", LoadBalanceStrategy: &strategy}}}
	key, err := client.APIKey.Create().SetName("old").SetKey("ah-test-only-sticky").SetType(apikey.TypeServiceAccount).SetUser(user).SetProject(project).SetProfiles(&profiles).Save(ctx)
	require.NoError(t, err)
	route := ModelStickyKey{key.ID, "A"}
	lease, err := ModelStickyRuntime.Acquire(ctx, route, "old", stickyCandidates(2)[1:], map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	name := "renamed"
	_, err = service.UpdateAPIKey(ctx, key.ID, ent.UpdateAPIKeyInput{Name: &name})
	require.NoError(t, err)
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
	profiles.ActiveProfile = "renamed-profile"
	profiles.Profiles[0].Name = profiles.ActiveProfile
	_, err = service.UpdateAPIKeyProfiles(ctx, key.ID, profiles)
	require.NoError(t, err)
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
	nextStrategy := LoadBalancerStrategyAdaptive
	profiles.Profiles[0].LoadBalanceStrategy = &nextStrategy
	_, err = service.UpdateAPIKeyProfiles(ctx, key.ID, profiles)
	require.NoError(t, err)
	require.Empty(t, ModelStickyRuntime.Snapshots())
	profiles.Profiles[0].LoadBalanceStrategy = &strategy
	_, err = service.UpdateAPIKeyProfiles(ctx, key.ID, profiles)
	require.NoError(t, err)
	require.Empty(t, ModelStickyRuntime.Snapshots())
}
