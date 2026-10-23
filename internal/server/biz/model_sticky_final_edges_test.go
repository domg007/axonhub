package biz

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/stretchr/testify/require"
)

func TestModelStickyFinalActualModelSyncRetiresOnlyRemovedModel(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint("manual=", manual), func(t *testing.T) {
			stickyFreshRuntime(t)
			var restore atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				if restore.Load() {
					_, _ = w.Write([]byte(`{"data":[{"id":"A"},{"id":"B"}]}`))
				} else {
					_, _ = w.Write([]byte(`{"data":[{"id":"B"}]}`))
				}
			}))
			t.Cleanup(server.Close)
			svc, client := setupTestChannelService(t)
			t.Cleanup(func() { svc.Stop(); client.Close() })
			svc.httpClient = httpclient.NewHttpClientWithClient(server.Client())
			svc.channelNotifier = &channelSyncNotifierSpy{}
			ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
			ch, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("sync-target").SetBaseURL(server.URL).SetCredentials(objects.ChannelCredentials{APIKey: "test-only"}).SetSupportedModels([]string{"A", "B"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).SetAutoSyncSupportedModels(true).Save(ctx)
			require.NoError(t, err)
			a := ModelStickyKey{1, "A"}
			b := ModelStickyKey{1, "B"}
			cs := []ModelStickyCandidate{{ID: ch.ID, Name: "a", Models: []ChannelModelEntry{{RequestModel: "A", ActualModel: "A"}}}, {ID: ch.ID + 1, Name: "b", Models: []ChannelModelEntry{{RequestModel: "A", ActualModel: "A"}}}, {ID: ch.ID + 2, Name: "c", Models: []ChannelModelEntry{{RequestModel: "A", ActualModel: "A"}}}}
			// Keep an unrelated failed channel cooldown that must survive model refresh.
			l, err := ModelStickyRuntime.Acquire(ctx, a, "key", cs[2:], map[int]bool{}, true)
			require.NoError(t, err)
			l.Failure()
			l, err = ModelStickyRuntime.Acquire(ctx, a, "key", cs, map[int]bool{}, true)
			require.NoError(t, err)
			l.Success()
			late, err := ModelStickyRuntime.Acquire(ctx, a, "key", cs, map[int]bool{}, true)
			require.NoError(t, err)
			if manual {
				require.NoError(t, ModelStickyRuntime.Control(a, ch.ID))
			}
			other, err := ModelStickyRuntime.Acquire(ctx, b, "key", []ModelStickyCandidate{{ID: ch.ID, Name: "a", Models: []ChannelModelEntry{{RequestModel: "B", ActualModel: "B"}}}}, map[int]bool{}, true)
			require.NoError(t, err)
			other.Success()
			// Scheduled path, not a synthetic manager reset or only the disable button.
			svc.syncChannelModels(ctx)
			saved, err := client.Channel.Get(ctx, ch.ID)
			require.NoError(t, err)
			require.Equal(t, []string{"B"}, saved.SupportedModels)
			require.False(t, late.Active())
			late.Success()
			next, err := ModelStickyRuntime.Acquire(ctx, a, "key", cs, map[int]bool{}, true)
			require.NoError(t, err)
			require.Equal(t, ch.ID+1, next.ChannelID)
			next.Success()
			other, err = ModelStickyRuntime.Acquire(ctx, b, "key", []ModelStickyCandidate{{ID: ch.ID, Name: "a", Models: []ChannelModelEntry{{RequestModel: "B", ActualModel: "B"}}}}, map[int]bool{}, true)
			require.NoError(t, err)
			require.Equal(t, ch.ID, other.ChannelID)
			other.Success()
			require.Len(t, ModelStickyRuntime.Snapshots()[0].Cooldowns, 1)
			restore.Store(true)
			svc.syncChannelModels(ctx)
			next, err = ModelStickyRuntime.Acquire(ctx, a, "key", cs, map[int]bool{}, true)
			require.NoError(t, err)
			require.Equal(t, ch.ID+1, next.ChannelID)
			next.Success()
			// A stale cache swap after the real save cannot resurrect old model state.
			ModelStickyRuntime.PublishChannel(ch)
			next, err = ModelStickyRuntime.Acquire(ctx, a, "key", cs, map[int]bool{}, true)
			require.NoError(t, err)
			require.Equal(t, ch.ID+1, next.ChannelID)
			next.Success()
		})
	}
}

func TestModelStickyFinalChannelDeleteAndRollback(t *testing.T) {
	stickyFreshRuntime(t)
	svc, client := setupTestChannelService(t)
	t.Cleanup(func() { svc.Stop(); client.Close() })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	ch, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("delete-target").SetBaseURL("https://example.test").SetCredentials(objects.ChannelCredentials{APIKey: "test-only"}).SetSupportedModels([]string{"A"}).SetDefaultTestModel("A").SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	key := ModelStickyKey{1, "A"}
	cs := []ModelStickyCandidate{{ID: ch.ID, Name: "a"}, {ID: ch.ID + 1, Name: "b"}}
	lease, err := ModelStickyRuntime.Acquire(ctx, key, "key", cs, map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	txctx := ent.NewContext(ent.NewTxContext(ctx, tx), tx.Client())
	_, err = svc.UpdateChannelStatus(txctx, ch.ID, channel.StatusDisabled)
	require.NoError(t, err)
	require.True(t, lease.Active())
	require.NoError(t, tx.Rollback())
	require.True(t, lease.Active())
	require.NoError(t, svc.DeleteChannel(ctx, ch.ID))
	require.False(t, lease.Active())
	_, err = ModelStickyRuntime.Acquire(ctx, key, "key", cs[:1], map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickyNoCandidates)
	next, err := ModelStickyRuntime.Acquire(ctx, key, "key", cs, map[int]bool{}, true)
	require.NoError(t, err)
	require.Equal(t, ch.ID+1, next.ChannelID)
	next.Success()
	require.Error(t, ModelStickyRuntime.Control(key, ch.ID))
}

func TestModelStickyFinalAdmissionBeforeFirstLeaseABAAndCacheFence(t *testing.T) {
	m := NewModelStickyManager()
	ctx := context.Background()
	key := &ent.APIKey{ID: 1, Name: "k", Status: apikey.StatusEnabled, ProjectID: 10}
	route := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	old := m.ConfigSnapshot(ctx)
	m.ResetKey(1)
	m.ResetKey(1) // A -> B -> A, including no route existing at either change.
	admission := NewModelStickyAdmission(old, key, false, "")
	_, err := m.Acquire(WithModelStickyAdmission(ctx, admission), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	require.Empty(t, m.Snapshots())
	current := *key
	current.Status = apikey.StatusDisabled
	m.PublishKey(&current)
	// New request generation + old cache object is still invalid.
	admission = NewModelStickyAdmission(m.ConfigSnapshot(ctx), key, false, "")
	_, err = m.Acquire(WithModelStickyAdmission(ctx, admission), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	// Unrelated key/model changes do not cancel this key's admission.
	admission = NewModelStickyAdmission(m.ConfigSnapshot(ctx), &current, false, "")
	m.ResetKey(2)
	m.ResetModel("B")
	m.ResetSystemInherited()
	lease, err := m.Acquire(WithModelStickyAdmission(ctx, admission), route, "k", cs, map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	beforeManual := NewModelStickyAdmission(m.ConfigSnapshot(ctx), &current, false, "")
	require.NoError(t, m.Control(route, 2))
	_, err = m.Acquire(WithModelStickyAdmission(ctx, beforeManual), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	// Stale quota evidence must not mutate a newly selected target before admission fails.
	beforeManual.QuotaExcludedChannelID = 2
	_, err = m.Acquire(WithModelStickyAdmission(ctx, beforeManual), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	require.Equal(t, 2, m.Snapshots()[0].PreferredChannelID)
}

func TestModelStickyFinalProjectAndSystemAdmissionIsolation(t *testing.T) {
	m := NewModelStickyManager()
	ctx := context.Background()
	project := &ent.Project{ID: 10}
	key := &ent.APIKey{ID: 1, ProjectID: 10}
	key.Edges.Project = project
	route := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	old := NewModelStickyAdmission(m.ConfigSnapshot(ctx), key, true, LoadBalancerStrategyModelSticky)
	m.PublishProject(project)
	_, err := m.Acquire(WithModelStickyAdmission(ctx, old), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	old = NewModelStickyAdmission(m.ConfigSnapshot(ctx), key, true, LoadBalancerStrategyModelSticky)
	m.PublishSystemStrategy(LoadBalancerStrategyAdaptive)
	m.PublishSystemStrategy(LoadBalancerStrategyModelSticky)
	_, err = m.Acquire(WithModelStickyAdmission(ctx, old), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
	// A current generation carrying a stale project cache fingerprint is rejected.
	changed := *project
	changed.Profiles = &objects.ProjectProfiles{ActiveProfile: "main", Profiles: []objects.ProjectProfile{{Name: "main", ChannelIDs: []int{2}}}}
	m.PublishProject(&changed)
	current := NewModelStickyAdmission(m.ConfigSnapshot(ctx), key, false, "")
	_, err = m.Acquire(WithModelStickyAdmission(ctx, current), route, "k", cs, map[int]bool{}, true)
	require.ErrorIs(t, err, ErrModelStickySettingsChanged)
}

func TestModelStickyFinalProfileCommitFailureAndMultipleSaves(t *testing.T) {
	for _, mode := range []string{"commit", "commit-failure", "multiple"} {
		t.Run(mode, func(t *testing.T) {
			svc, client, ctx, key := review2Key(t)
			tx, err := client.Tx(ctx)
			require.NoError(t, err)
			txctx := ent.NewContext(ent.NewTxContext(ctx, tx), tx.Client())
			strategy := LoadBalancerStrategyAdaptive
			profiles := objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", LoadBalanceStrategy: &strategy}}}
			_, err = svc.UpdateAPIKeyProfiles(txctx, key.ID, profiles)
			require.NoError(t, err)
			require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
			if mode == "multiple" {
				strategy = LoadBalancerStrategyModelSticky
				profiles.Profiles[0].LoadBalanceStrategy = &strategy
				_, err = svc.UpdateAPIKeyProfiles(txctx, key.ID, profiles)
				require.NoError(t, err)
			}
			if mode == "commit-failure" {
				tx.OnCommit(func(next ent.Committer) ent.Committer {
					return ent.CommitFunc(func(context.Context, *ent.Tx) error { return errors.New("injected commit failure") })
				})
				require.Error(t, tx.Commit())
				require.NoError(t, tx.Rollback())
				require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
				return
			}
			require.NoError(t, tx.Commit())
			if mode == "multiple" {
				require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
			} else {
				require.Empty(t, ModelStickyRuntime.Snapshots())
			}
			saved, err := client.APIKey.Get(ctx, key.ID)
			require.NoError(t, err)
			if mode == "multiple" {
				// Fingerprint callback must have published final committed value, not the intermediate adaptive profile.
				admission := NewModelStickyAdmission(ModelStickyRuntime.ConfigSnapshot(ctx), saved, false, "")
				lease, err := ModelStickyRuntime.Acquire(WithModelStickyAdmission(ctx, admission), ModelStickyKey{key.ID, "A"}, "k", stickyCandidates(2), map[int]bool{}, true)
				require.NoError(t, err)
				lease.Success()
			}
		})
	}
}

func TestModelStickyFinalBulkKeyNoopRollbackEnableArchive(t *testing.T) {
	svc, client, ctx, key := review2Key(t)
	require.NoError(t, svc.BulkEnableAPIKeys(ctx, []int{key.ID}))
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	txctx := ent.NewContext(ent.NewTxContext(ctx, tx), tx.Client())
	require.NoError(t, svc.BulkDisableAPIKeys(txctx, []int{key.ID}))
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
	require.NoError(t, tx.Rollback())
	require.Equal(t, 2, ModelStickyRuntime.Snapshots()[0].ChannelID)
	require.NoError(t, svc.BulkDisableAPIKeys(ctx, []int{key.ID}))
	require.Empty(t, ModelStickyRuntime.Snapshots())
	require.NoError(t, svc.BulkEnableAPIKeys(ctx, []int{key.ID}))
	require.Empty(t, ModelStickyRuntime.Snapshots())
	l, err := ModelStickyRuntime.Acquire(ctx, ModelStickyKey{key.ID, "A"}, "k", stickyCandidates(2), map[int]bool{}, true)
	require.NoError(t, err)
	l.Success()
	require.NoError(t, svc.bulkUpdateAPIKeyStatus(ctx, []int{key.ID}, apikey.StatusArchived, "archive"))
	require.False(t, l.Active())
	require.Empty(t, ModelStickyRuntime.Snapshots())
}

func TestModelStickyFinalPartialModelRemovalFencesOldLease(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Now()
	ctx := context.Background()
	k := ModelStickyKey{1, "A"}
	cs := []ModelStickyCandidate{{ID: 1, Name: "a", Models: []ChannelModelEntry{{RequestModel: "x", ActualModel: "x"}, {RequestModel: "y", ActualModel: "y"}}}, {ID: 2, Name: "b"}}
	l, err := m.Acquire(ctx, k, "k", cs, map[int]bool{}, true)
	require.NoError(t, err)
	l.Success()
	late, err := m.Acquire(ctx, k, "k", cs, map[int]bool{}, true)
	require.NoError(t, err)
	m.PublishChannel(&ent.Channel{ID: 1, Status: channel.StatusEnabled, UpdatedAt: now, SupportedModels: []string{"y"}})
	require.False(t, late.Active())
	late.Failure()
	next, err := m.Acquire(ctx, k, "k", cs, map[int]bool{}, true)
	require.NoError(t, err)
	require.Equal(t, 1, next.ChannelID)
	require.Equal(t, []ChannelModelEntry{{RequestModel: "y", ActualModel: "y"}}, next.Models)
	next.Success()
	// Repeating an unchanged refresh should not invalidate the new legal lease.
	m.PublishChannel(&ent.Channel{ID: 1, Status: channel.StatusEnabled, UpdatedAt: now, SupportedModels: []string{"y"}})
	require.True(t, next.Active())
}

// A request-specific incompatibility is not a configuration update. It must
// retain the association used to invalidate the target on later real removal.
func TestModelStickyFinalConflictThenActualTargetRemoval(t *testing.T) {
	for _, manual := range []bool{false, true} {
		for _, removedModel := range []bool{false, true} {
			t.Run(fmt.Sprintf("manual=%v/model-removal=%v", manual, removedModel), func(t *testing.T) {
				m := NewModelStickyManager()
				key := ModelStickyKey{APIKeyID: 1, Model: "A"}
				entries := []ChannelModelEntry{{RequestModel: "A", ActualModel: "A"}}
				candidates := []ModelStickyCandidate{{ID: 1, Models: entries}, {ID: 2, Models: entries}}
				target := &ent.Channel{ID: 1, Status: channel.StatusEnabled, SupportedModels: []string{"A"}, UpdatedAt: time.Now()}
				m.PublishChannel(target)
				l, err := m.Acquire(context.Background(), key, "key", candidates, map[int]bool{}, true)
				require.NoError(t, err)
				l.Success()
				if manual {
					require.NoError(t, m.Control(key, 1))
				}
				_, err = m.Acquire(context.Background(), key, "key", candidates[1:], map[int]bool{}, true)
				require.ErrorIs(t, err, ErrModelStickyRequestConflict)
				target.UpdatedAt = target.UpdatedAt.Add(time.Second)
				if removedModel {
					target.SupportedModels = []string{"B"}
				} else {
					target.Status = channel.StatusDisabled
				}
				m.PublishChannel(target)
				next, err := m.Acquire(context.Background(), key, "key", candidates[1:], map[int]bool{}, true)
				require.NoError(t, err, "actual target removal must still retire pin/preferred after an earlier request-specific conflict")
				require.Equal(t, 2, next.ChannelID)
				next.Success()
			})
		}
	}
}
