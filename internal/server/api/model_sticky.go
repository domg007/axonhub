package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/server/biz"
)

// ModelStickyHandlers exposes only non-secret, process-local routing metadata.
// All routes require JWT authentication AND workspace-owner authorization.
type ModelStickyHandlers struct{}

func NewModelStickyHandlers() *ModelStickyHandlers { return &ModelStickyHandlers{} }
func (h *ModelStickyHandlers) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/model-sticky", h.List)
	group.POST("/model-sticky", h.Control)
}
func modelStickyOwner(c *gin.Context) bool {
	user, ok := contexts.GetUser(c.Request.Context())
	if !ok || !user.IsOwner {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "【新增策略：模型固定渠道】此管理操作需要工作区所有者权限。"})
		return false
	}
	return true
}
func (h *ModelStickyHandlers) List(c *gin.Context) {
	if !modelStickyOwner(c) {
		return
	}
	c.JSON(http.StatusOK, biz.ModelStickyRuntime.Snapshots())
}
func (h *ModelStickyHandlers) Control(c *gin.Context) {
	if !modelStickyOwner(c) {
		return
	}
	var input struct {
		APIKeyID  int    `json:"apiKeyID" binding:"required,min=1"`
		Model     string `json:"model" binding:"required"`
		ChannelID *int   `json:"channelID" binding:"required,min=0"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "【新增策略：模型固定渠道】切换参数无效，请检查 Key、模型及渠道信息。"})
		return
	}
	if err := biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: input.APIKeyID, Model: input.Model}, *input.ChannelID); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}
