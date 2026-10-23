package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestModelStickyControlRequiresExplicitChannelID(t *testing.T) {
	for _, body := range []string{`{"apiKeyID":42,"model":"A"}`, `{"apiKeyID":42,"model":"A","channelID":null}`, `{"apiKeyID":42,"model":"A","channelID":0}`, `{"apiKeyID":42,"model":"A","channelID":-1}`, `{"apiKeyID":42,"model":"A","channelID":999}`} {
		t.Run(body, func(t *testing.T) {
			biz.ModelStickyRuntime.ResetAll()
			t.Cleanup(biz.ModelStickyRuntime.ResetAll)
			key := biz.ModelStickyKey{APIKeyID: 42, Model: "A"}
			candidates := []biz.ModelStickyCandidate{{ID: 1}, {ID: 2}}
			first, err := biz.ModelStickyRuntime.Acquire(context.Background(), key, "review", candidates, nil, true)
			require.NoError(t, err)
			first.Failure()
			second, err := biz.ModelStickyRuntime.Acquire(context.Background(), key, "review", candidates, nil, true)
			require.NoError(t, err)
			second.Success()
			before := biz.ModelStickyRuntime.Snapshots()[0]
			require.Equal(t, 2, before.ChannelID)
			require.Len(t, before.Cooldowns, 1)
			router := gin.New()
			NewModelStickyHandlers().RegisterRoutes(router.Group("/admin"))
			req := httptest.NewRequest(http.MethodPost, "/admin/model-sticky", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req = req.WithContext(contexts.WithUser(req.Context(), &ent.User{IsOwner: true}))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			after := biz.ModelStickyRuntime.Snapshots()[0]
			if strings.Contains(body, `"channelID":0`) {
				require.Equal(t, 200, w.Code)
				require.Zero(t, after.ChannelID)
				require.Empty(t, after.Cooldowns)
			} else {
				expected := http.StatusBadRequest
				if strings.Contains(body, `"channelID":999`) {
					expected = http.StatusConflict
				}
				require.Equal(t, expected, w.Code)
				require.Equal(t, before, after)
				require.Contains(t, w.Body.String(), biz.ModelStickyMessagePrefix)
			}
		})
	}
}
