package biz

import (
	"context"
	"testing"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/stretchr/testify/require"
)

func TestModelStickyProfileFingerprintIgnoresNamesInactiveDataAndSetOrder(t *testing.T) {
	strategy := LoadBalancerStrategyModelSticky
	a := &objects.APIKeyProfile{Name: "old", ChannelIDs: []int{2, 1, 1}, ChannelTags: []string{"b", "a"}, LoadBalanceStrategy: &strategy}
	b := a.Clone()
	b.Name = "new"
	b.TemplateName = "renamed template"
	b.ChannelIDs = []int{1, 2}
	b.ChannelTags = []string{"a", "b"}
	b.ChannelTagsMatchMode = objects.ChannelTagsMatchModeAny
	require.Equal(t, modelStickyProfileFingerprint(a), modelStickyProfileFingerprint(b))
	b.ChannelIDs = []int{1}
	require.NotEqual(t, modelStickyProfileFingerprint(a), modelStickyProfileFingerprint(b))
	require.Equal(t, modelStickyProfileFingerprint(nil), modelStickyProfileFingerprint(&objects.APIKeyProfile{}))
}
func TestModelStickyKeyRenameAndUnchangedModelSettingsKeepFingerprint(t *testing.T) {
	a := &ent.APIKey{Name: "old"}
	b := *a
	b.Name = "new"
	require.Equal(t, modelStickyKeyFingerprint(a), modelStickyKeyFingerprint(&b))
	require.Equal(t, modelStickyModelFingerprint(nil), modelStickyModelFingerprint(&objects.ModelSettings{}))
	require.NotEqual(t, modelStickyModelFingerprint(nil), modelStickyModelFingerprint(&objects.ModelSettings{LoadBalancerStrategy: LoadBalancerStrategyModelSticky}))
}
func TestModelStickyGlobalStrategyChangePreservesExplicitOverrideAndManualTarget(t *testing.T) {
	m := NewModelStickyManager()
	ctx := context.Background()
	cs := stickyCandidates(2)
	override, err := m.Acquire(ctx, ModelStickyKey{1, "A"}, "override", cs, map[int]bool{}, true)
	require.NoError(t, err)
	override.SetSystemInherited(false)
	override.Success()
	inherited, err := m.Acquire(ctx, ModelStickyKey{2, "A"}, "inherited", cs, map[int]bool{}, true)
	require.NoError(t, err)
	inherited.Success()
	require.NoError(t, m.Control(ModelStickyKey{1, "A"}, 2))
	m.ResetSystemInherited()
	rows := m.Snapshots()
	require.Len(t, rows, 1)
	require.Equal(t, 1, rows[0].APIKeyID)
	next, err := m.Acquire(ctx, ModelStickyKey{1, "A"}, "override", cs, map[int]bool{}, true)
	require.NoError(t, err)
	require.Equal(t, 2, next.ChannelID)
	next.Success()
}
