package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/stretchr/testify/require"
)

func TestModelStickyAdminErrorsHaveChineseSource(t *testing.T) {
	biz.ModelStickyRuntime.ResetAll()
	t.Cleanup(biz.ModelStickyRuntime.ResetAll)
	lease, err := biz.ModelStickyRuntime.Acquire(context.Background(), biz.ModelStickyKey{APIKeyID: 42, Model: "A"}, "safe-name", []biz.ModelStickyCandidate{{ID: 1}}, nil, true)
	require.NoError(t, err)
	lease.Success()
	router := gin.New()
	NewModelStickyHandlers().RegisterRoutes(router.Group("/admin"))
	for _, tc := range []struct {
		name, body string
		owner      bool
		status     int
		message    string
	}{
		{"authorization", `{}`, false, 403, "所有者权限"},
		{"malformed-json", `{"model":`, true, 400, "切换参数无效"},
		{"negative-channel", `{"apiKeyID":42,"model":"A","channelID":-1}`, true, 400, "切换参数无效"},
		{"missing-state", `{"apiKeyID":43,"model":"A","channelID":1}`, true, 409, "刷新"},
		{"outside-authorized-candidates", `{"apiKeyID":42,"model":"A","channelID":99}`, true, 409, "授权候选"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/admin/model-sticky", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req = req.WithContext(contexts.WithUser(req.Context(), &ent.User{IsOwner: tc.owner}))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code)
			var body struct {
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.True(t, strings.HasPrefix(body.Error, biz.ModelStickyMessagePrefix))
			require.Contains(t, body.Error, tc.message)
			require.NotContains(t, body.Error, "Key:")
			require.NotContains(t, body.Error, "validation")
		})
	}
}
