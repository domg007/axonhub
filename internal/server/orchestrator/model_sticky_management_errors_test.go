package orchestrator

import (
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// Default management errors retain upstream provenance and error chains.
func TestModelStickyDefaultManagementErrorsRemainOriginal(t *testing.T) {
	stickyFreshRuntime(t)
	ctx, db := setupTest(t)
	ss := newTestSystemService(db)
	require.NoError(t, ss.SetRetryPolicy(ctx, &biz.RetryPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive}))
	require.Empty(t, biz.ModelStickyRuntime.Snapshots())
	_, err := newTestModelService(db).UpdateModel(ctx, 999999, &ent.UpdateModelInput{Settings: &objects.ModelSettings{}})
	require.Error(t, err)
	require.True(t, ent.IsNotFound(err))
	require.False(t, strings.HasPrefix(err.Error(), biz.ModelStickyMessagePrefix))
	ps := biz.NewProjectService(biz.ProjectServiceParams{Ent: db})
	_, err = ps.UpdateProjectProfiles(ctx, 999999, objects.ProjectProfiles{})
	require.Error(t, err)
	require.True(t, ent.IsNotFound(err))
	require.False(t, strings.HasPrefix(err.Error(), biz.ModelStickyMessagePrefix))
	require.Empty(t, biz.ModelStickyRuntime.Snapshots())
}
