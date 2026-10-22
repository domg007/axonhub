package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/pkg/xcache/live"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/stretchr/testify/require"
)

// Exercise the real HTTP authentication surface. The cache loader changes the
// configuration after middleware entry but before returning the authenticated
// key, with no existing route. Capturing only at Process/Acquire is too late.
func TestModelStickyAuthSnapshotPrecedesKeyCacheRead(t *testing.T) {
	for _, gemini := range []bool{false, true} {
		t.Run(map[bool]string{false: "api-key", true: "gemini"}[gemini], func(t *testing.T) {
			previous := biz.ModelStickyRuntime
			biz.ModelStickyRuntime = biz.NewModelStickyManager()
			t.Cleanup(func() { biz.ModelStickyRuntime.ResetAll(); biz.ModelStickyRuntime = previous })
			client := enttest.NewEntClient(t, "sqlite3", "file:sticky-auth?mode=memory&_fk=1")
			t.Cleanup(func() { _ = client.Close() })
			ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
			user, err := client.User.Create().SetEmail("sticky-auth@example.test").SetPassword("test-only").Save(ctx)
			require.NoError(t, err)
			project, err := client.Project.Create().SetName("sticky-auth").Save(ctx)
			require.NoError(t, err)
			key, err := client.APIKey.Create().SetName("sticky-auth").SetKey("test-only").SetType(apikey.TypeServiceAccount).SetUser(user).SetProject(project).Save(ctx)
			require.NoError(t, err)
			var once sync.Once
			cache := live.NewIndexedCache(live.IndexedOptions[string, *ent.APIKey]{
				Name: "sticky-auth", TTL: time.Minute,
				RefreshInterval: time.Minute,
				LoadSinceFunc:   func(context.Context, time.Time) ([]*ent.APIKey, time.Time, error) { return nil, time.Time{}, nil },
				KeyFunc:         func(*ent.APIKey) string { return "test-only" },
				LoadOneFunc: func(context.Context, string) (*ent.APIKey, error) {
					once.Do(func() { biz.ModelStickyRuntime.ResetKey(key.ID); biz.ModelStickyRuntime.ResetKey(key.ID) })
					return key, nil
				},
			})
			t.Cleanup(cache.Stop)
			service := &biz.AuthService{APIKeyService: &biz.APIKeyService{
				APIKeyCache:    cache,
				ProjectService: biz.NewProjectService(biz.ProjectServiceParams{Ent: client}),
			}}
			router := gin.New()
			auth := WithAPIKeyAuth(service)
			if gemini {
				auth = WithGeminiKeyAuth(service)
			}
			attempts := 0
			router.GET("/test", auth, func(c *gin.Context) {
				attempts++
				requestCtx := c.Request.Context()
				current, ok := contexts.GetAPIKey(requestCtx)
				require.True(t, ok)
				admission := biz.NewModelStickyAdmission(biz.ModelStickyRuntime.ConfigSnapshot(requestCtx), current, false, biz.LoadBalancerStrategyModelSticky)
				lease, err := biz.ModelStickyRuntime.Acquire(biz.WithModelStickyAdmission(requestCtx, admission), biz.ModelStickyKey{APIKeyID: key.ID, Model: "A"}, key.Name, []biz.ModelStickyCandidate{{ID: 1}}, map[int]bool{}, true)
				if attempts == 1 {
					require.ErrorIs(t, err, biz.ErrModelStickySettingsChanged)
					require.Nil(t, lease)
					require.Empty(t, biz.ModelStickyRuntime.Snapshots())
				} else {
					require.NoError(t, err)
					if lease != nil {
						lease.Success()
					}
				}
				c.Status(http.StatusOK)
			})
			for i := 0; i < 2; i++ {
				url := "/test"
				if gemini {
					url += "?key=test-only"
				}
				req := httptest.NewRequest(http.MethodGet, url, nil).WithContext(ent.NewContext(context.Background(), client))
				req.Header.Set("Authorization", "Bearer test-only")
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, req)
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			}
			require.Equal(t, 2, attempts)
		})
	}
}
