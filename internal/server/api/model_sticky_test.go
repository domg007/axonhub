package api

import (
	"context"
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

func TestModelStickyAdminAuthorizationAndManualControl(t *testing.T) {
	biz.ModelStickyRuntime.ResetAll()
	t.Cleanup(biz.ModelStickyRuntime.ResetAll)
	key := biz.ModelStickyKey{APIKeyID: 42, Model: "A"}
	lease, err := biz.ModelStickyRuntime.Acquire(context.Background(), key, "public-key-name", []biz.ModelStickyCandidate{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}, map[int]bool{}, true)
	require.NoError(t, err)
	lease.Success()
	router := gin.New()
	NewModelStickyHandlers().RegisterRoutes(router.Group("/admin"))
	request := func(owner *bool, method, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/admin/model-sticky", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if owner != nil {
			req = req.WithContext(contexts.WithUser(req.Context(), &ent.User{IsOwner: *owner}))
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	yes, no := true, false
	for _, owner := range []*bool{nil, &no} {
		require.Equal(t, http.StatusForbidden, request(owner, http.MethodGet, "").Code)
		require.Equal(t, http.StatusForbidden, request(owner, http.MethodPost, `{"apiKeyID":42,"model":"A","channelID":2}`).Code)
	}
	response := request(&yes, http.MethodGet, "")
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), "public-key-name")
	require.NotContains(t, response.Body.String(), "api_key")
	require.Equal(t, 409, request(&yes, http.MethodPost, `{"apiKeyID":42,"model":"A","channelID":99}`).Code)
	require.Equal(t, 400, request(&yes, http.MethodPost, `{"apiKeyID":42,"model":"A","channelID":-1}`).Code)
	require.Equal(t, 200, request(&yes, http.MethodPost, `{"apiKeyID":42,"model":"A","channelID":2}`).Code)
	next, err := biz.ModelStickyRuntime.Acquire(context.Background(), key, "test", []biz.ModelStickyCandidate{{ID: 1}, {ID: 2}}, map[int]bool{}, true)
	require.NoError(t, err)
	require.Equal(t, 2, next.ChannelID)
	next.Abandon()
}
