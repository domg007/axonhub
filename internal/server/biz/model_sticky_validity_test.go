package biz

import (
	"context"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/stretchr/testify/require"
)

func validityAdmission(m *ModelStickyManager, key *ent.APIKey, entity *ent.Model, cs []ModelStickyCandidate, systemStrategy string) context.Context {
	ctx := context.Background()
	a := NewModelStickyAdmission(m.ConfigSnapshot(ctx), key, modelStickyInheritsSystem(modelStickyKeyStrategy(key), modelStickyStrategy(entity.Settings)), systemStrategy)
	a.ModelEntityID = entity.ID
	a.ModelFingerprint = ModelStickyModelFingerprint(entity)
	a.ModelStrategy = modelStickyStrategy(entity.Settings)
	a.StructuralCandidates = cs
	return WithModelStickyAdmission(ctx, a)
}

func TestModelStickyValidityInheritedSystemSurvivesModelAndKeyEdits(t *testing.T) {
	m := NewModelStickyManager()
	key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled}
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{}}
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(3)
	acquire := func() *ModelStickyLease {
		l, err := m.Acquire(validityAdmission(m, key, entity, cs, LoadBalancerStrategyModelSticky), k, "k", cs, nil, true)
		require.NoError(t, err)
		return l
	}
	first := acquire()
	first.Failure()
	second := acquire()
	require.Equal(t, 2, second.ChannelID)
	second.Success()
	before := m.Snapshots()[0]
	// No system publication is required before the first request: the effective
	// request policy itself supplies the inherited system strategy.
	entity.Developer = "new-developer"
	m.PublishModel(entity, entity.ID)
	require.Equal(t, before.ChannelID, m.Snapshots()[0].ChannelID)
	require.Equal(t, before.Cooldowns, m.Snapshots()[0].Cooldowns)
	third := acquire()
	require.Equal(t, 2, third.ChannelID)
	// The real key notification also keeps this inherited effective strategy.
	key.Profiles = &objects.APIKeyProfiles{ActiveProfile: "main", Profiles: []objects.APIKeyProfile{{Name: "main", ChannelIDs: []int{1, 2, 3}}}}
	m.PublishKey(key)
	require.False(t, third.Active())
	require.Equal(t, 2, m.Snapshots()[0].ChannelID)
	require.Equal(t, before.Cooldowns, m.Snapshots()[0].Cooldowns)
	acquire().Success()
	m.PublishSystemStrategy(LoadBalancerStrategyAdaptive)
	require.Empty(t, m.Snapshots())
}

func TestModelStickyValidityChannelPublicationCannotClearPermissionPending(t *testing.T) {
	m := NewModelStickyManager()
	k := ModelStickyKey{1, "A"}
	l, err := m.Acquire(context.Background(), k, "k", stickyCandidates(2), nil, true)
	require.NoError(t, err)
	l.Success()
	m.PublishKey(&ent.APIKey{ID: 1, Status: apikey.StatusEnabled})
	require.True(t, m.routes[k].validationPending)
	// Channel metadata alone cannot vouch for the latest Key/project permissions.
	m.mu.Lock()
	m.publishChannelLocked(1, modelStickyChannelState{enabled: true})
	m.mu.Unlock()
	require.True(t, m.routes[k].validationPending)
	require.Error(t, m.Control(k, 1))
	require.Equal(t, 1, m.Snapshots()[0].ChannelID)
}

func TestModelStickyValidityDuplicateAndOlderPublicationsDoNotFenceFreshLease(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled, UpdatedAt: now}
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, UpdatedAt: now, Settings: &objects.ModelSettings{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}}
	m.PublishKey(key)
	m.PublishModel(entity, 10)
	m.publishInheritance("[]", now)
	a := NewModelStickyAdmission(m.ConfigSnapshot(context.Background()), key, false, "")
	a.ModelEntityID = 10
	a.ModelFingerprint = ModelStickyModelFingerprint(entity)
	a.ModelStrategy = LoadBalancerStrategyModelSticky
	a.InheritanceFingerprint = "[]"
	a.StructuralCandidates = stickyCandidates(2)
	l, err := m.Acquire(WithModelStickyAdmission(context.Background(), a), ModelStickyKey{1, "A"}, "k", stickyCandidates(2), nil, true)
	require.NoError(t, err)
	m.PublishKey(key)
	m.PublishModel(entity, 10)
	m.publishInheritance("[]", now)
	require.True(t, l.Active())
	oldKey := *key
	oldKey.UpdatedAt = now.Add(-time.Second)
	oldKey.Status = apikey.StatusDisabled
	oldModel := *entity
	oldModel.UpdatedAt = oldKey.UpdatedAt
	oldModel.Status = model.StatusDisabled
	m.PublishKey(&oldKey)
	m.PublishModel(&oldModel, 10)
	m.publishInheritance("old", oldKey.UpdatedAt)
	require.True(t, l.Active())
	l.Success()
	require.Equal(t, 1, m.Snapshots()[0].ChannelID)
}

func TestModelStickyValidityStructuralRemovalVersusRequestFilter(t *testing.T) {
	m := NewModelStickyManager()
	key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled}
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}}
	k := ModelStickyKey{1, "A"}
	cs := stickyCandidates(2)
	l, err := m.Acquire(validityAdmission(m, key, entity, cs, ""), k, "k", cs, nil, true)
	require.NoError(t, err)
	l.Success()
	// It still belongs to A, but this request cannot use it. Keep the healthy pin.
	_, err = m.Acquire(validityAdmission(m, key, entity, cs, ""), k, "k", cs[1:], nil, true)
	require.ErrorIs(t, err, ErrModelStickyRequestConflict)
	require.Equal(t, 1, m.Snapshots()[0].ChannelID)
	// Latest authorized structural facts remove channel 1: relay, not 409.
	l, err = m.Acquire(validityAdmission(m, key, entity, cs[1:], ""), k, "k", cs[1:], nil, true)
	require.NoError(t, err)
	require.Equal(t, 2, l.ChannelID)
	l.Success()
	require.Empty(t, m.Snapshots()[0].Cooldowns)
}

func TestModelStickyValidityDeletionAfterPriorPublicationIsNotDeduplicated(t *testing.T) {
	for _, hardDelete := range []bool{false, true} {
		name := "soft-delete"
		if hardDelete {
			name = "missing-row"
		}
		t.Run(name, func(t *testing.T) {
			m := NewModelStickyManager()
			key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled}
			entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}}
			m.PublishModel(entity, 10)
			l, err := m.Acquire(validityAdmission(m, key, entity, stickyCandidates(2), ""), ModelStickyKey{1, "A"}, "k", stickyCandidates(2), nil, true)
			require.NoError(t, err)
			if hardDelete {
				m.PublishModel(nil, 10)
			} else {
				entity.DeletedAt = 1
				m.PublishModel(entity, 10)
			}
			require.False(t, l.Active())
			l.Success()
			require.Empty(t, m.Snapshots())
			_, err = m.Acquire(validityAdmission(m, key, entity, stickyCandidates(2), ""), ModelStickyKey{1, "A"}, "k", stickyCandidates(2), nil, true)
			require.ErrorIs(t, err, ErrModelStickySettingsChanged)
		})
	}
}

func TestModelStickyValidityFirstSystemSaveDoesNotResetSameEffectivePolicy(t *testing.T) {
	m := NewModelStickyManager()
	key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled}
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{}}
	cs := stickyCandidates(2)
	l, err := m.Acquire(validityAdmission(m, key, entity, cs, LoadBalancerStrategyModelSticky), ModelStickyKey{1, "A"}, "k", cs, nil, true)
	require.NoError(t, err)
	l.Success()
	m.PublishSystemStrategy(LoadBalancerStrategyModelSticky)
	require.Equal(t, 1, m.Snapshots()[0].ChannelID)
	m.PublishSystemStrategy(LoadBalancerStrategyAdaptive)
	require.Empty(t, m.Snapshots())
}

func TestModelStickyValidityMaintenanceFailureFencesWithoutErasingHealth(t *testing.T) {
	for _, scope := range []string{"key", "project", "system"} {
		t.Run(scope, func(t *testing.T) {
			m := NewModelStickyManager()
			key := &ent.APIKey{ID: 1, ProjectID: 7, Status: apikey.StatusEnabled}
			entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{}}
			k := ModelStickyKey{1, "A"}
			cs := stickyCandidates(2)
			acquire := func() *ModelStickyLease {
				l, err := m.Acquire(validityAdmission(m, key, entity, cs, LoadBalancerStrategyModelSticky), k, "k", cs, nil, true)
				require.NoError(t, err)
				return l
			}
			acquire().Failure()
			acquire().Success()
			old := acquire()
			before := m.Snapshots()[0]
			switch scope {
			case "key":
				m.fenceKeys([]int{1})
			case "project":
				m.fenceProject(7)
			case "system":
				m.fenceSystemInherited()
			}
			require.False(t, old.Active())
			require.Equal(t, before.ChannelID, m.Snapshots()[0].ChannelID)
			require.Equal(t, before.Cooldowns, m.Snapshots()[0].Cooldowns)
			fresh := acquire()
			require.Equal(t, 2, fresh.ChannelID)
			fresh.Success()
		})
	}
}

func TestModelStickyValidityFirstNoopPublicationsKeepInFlightProbe(t *testing.T) {
	m := NewModelStickyManager()
	key := &ent.APIKey{ID: 1, ProjectID: 7, Status: apikey.StatusEnabled}
	project := &ent.Project{ID: 7}
	key.Edges.Project = project
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}}
	cs := stickyCandidates(2)
	l, err := m.Acquire(validityAdmission(m, key, entity, cs, ""), ModelStickyKey{1, "A"}, "k", cs, nil, true)
	require.NoError(t, err)
	key.Name = "renamed key"
	entity.Name = "renamed display label"
	project.Name = "renamed project"
	m.PublishKey(key)
	m.PublishModel(entity, 10)
	m.PublishProject(project)
	require.True(t, l.Active())
	l.Success()
	require.Equal(t, 1, m.Snapshots()[0].ChannelID)
	// Renaming the model identifier is different from its display label.
	entity.ModelID = "B"
	m.PublishModel(entity, 10)
	require.Empty(t, m.Snapshots())
}

func TestModelStickyValidityOlderSystemPublicationCannotRevertPolicy(t *testing.T) {
	m := NewModelStickyManager()
	now := time.Unix(1000, 0)
	m.publishSystemStrategy(LoadBalancerStrategyModelSticky, now)
	key := &ent.APIKey{ID: 1, Status: apikey.StatusEnabled}
	entity := &ent.Model{ID: 10, ModelID: "A", Status: model.StatusEnabled, Settings: &objects.ModelSettings{}}
	cs := stickyCandidates(2)
	l, err := m.Acquire(validityAdmission(m, key, entity, cs, LoadBalancerStrategyModelSticky), ModelStickyKey{1, "A"}, "k", cs, nil, true)
	require.NoError(t, err)
	m.publishSystemStrategy(LoadBalancerStrategyAdaptive, now.Add(-time.Second))
	require.True(t, l.Active())
	m.publishSystemStrategy(LoadBalancerStrategyAdaptive, now.Add(time.Second))
	require.False(t, l.Active())
	l.Success()
	require.Empty(t, m.Snapshots())
}
