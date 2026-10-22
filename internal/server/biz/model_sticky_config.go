package biz

import (
	"context"

	"github.com/looplj/axonhub/internal/ent"
)

// Revisions fence ABA changes even when a route has not yet been created.
// These process-local tombstones are intentionally independent of live routes.
type modelStickyConfigState struct {
	revision, allChanged, systemChanged uint64
	keys                                map[int]uint64
	models                              map[string]uint64
	projects                            map[int]uint64
	controls                            map[ModelStickyKey]uint64
	keyFingerprints                     map[int]string
	projectFingerprints                 map[int]string
	systemStrategy                      string
}
type ModelStickyConfigSnapshot struct {
	manager  *ModelStickyManager
	revision uint64
}
type modelStickyConfigContextKey struct{}
type modelStickyAdmissionContextKey struct{}
type ModelStickyAdmission struct {
	QuotaExcludedChannelID                             int
	snapshot                                           *ModelStickyConfigSnapshot
	keyFingerprint, projectFingerprint, systemStrategy string
	projectID                                          int
	inherited                                          bool
}

// CaptureModelStickyConfig runs BEFORE authentication reads API key/project caches.
// Non-sticky strategies never inspect this metadata or change their routing.
func CaptureModelStickyConfig(ctx context.Context) context.Context {
	if _, ok := ctx.Value(modelStickyConfigContextKey{}).(*ModelStickyConfigSnapshot); ok {
		return ctx
	}
	return context.WithValue(ctx, modelStickyConfigContextKey{}, ModelStickyRuntime.ConfigSnapshot(ctx))
}
func (m *ModelStickyManager) ConfigSnapshot(ctx context.Context) *ModelStickyConfigSnapshot {
	if s, ok := ctx.Value(modelStickyConfigContextKey{}).(*ModelStickyConfigSnapshot); ok && s.manager == m {
		return s
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return &ModelStickyConfigSnapshot{manager: m, revision: m.config.revision}
}
func NewModelStickyAdmission(snapshot *ModelStickyConfigSnapshot, key *ent.APIKey, inherited bool, strategy string) *ModelStickyAdmission {
	a := &ModelStickyAdmission{snapshot: snapshot, inherited: inherited, systemStrategy: strategy, keyFingerprint: modelStickyKeyFingerprint(key), projectID: key.ProjectID}
	if key.Edges.Project != nil {
		a.projectFingerprint = modelStickyProjectFingerprint(key.Edges.Project)
	}
	return a
}
func WithModelStickyAdmission(ctx context.Context, a *ModelStickyAdmission) context.Context {
	if a == nil {
		return ctx
	}
	return context.WithValue(ctx, modelStickyAdmissionContextKey{}, a)
}
func (m *ModelStickyManager) nextConfigRevisionLocked() uint64 {
	m.config.revision++
	return m.config.revision
}
func (m *ModelStickyManager) admissionValidLocked(ctx context.Context, key ModelStickyKey) bool {
	a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission)
	if !ok {
		return true
	} // direct state-machine callers; production always supplies admission
	if a.snapshot == nil || a.snapshot.manager != m {
		return false
	}
	rev := a.snapshot.revision
	c := &m.config
	if c.allChanged > rev || c.keys[key.APIKeyID] > rev || c.models[key.Model] > rev || c.projects[a.projectID] > rev || c.controls[key] > rev || (a.inherited && c.systemChanged > rev) {
		return false
	}
	if expected, ok := c.keyFingerprints[key.APIKeyID]; ok && expected != a.keyFingerprint {
		return false
	}
	if expected, ok := c.projectFingerprints[a.projectID]; ok && expected != a.projectFingerprint {
		return false
	}
	return !a.inherited || c.systemStrategy == "" || c.systemStrategy == a.systemStrategy
}
func (m *ModelStickyManager) PublishKey(key *ent.APIKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.keyFingerprints == nil {
		m.config.keyFingerprints = make(map[int]string)
	}
	m.config.keyFingerprints[key.ID] = modelStickyKeyFingerprint(key)
	m.resetKeyLocked(key.ID)
}
func (m *ModelStickyManager) PublishProject(project *ent.Project) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.projects == nil {
		m.config.projects = make(map[int]uint64)
	}
	if m.config.projectFingerprints == nil {
		m.config.projectFingerprints = make(map[int]string)
	}
	m.config.projectFingerprints[project.ID] = modelStickyProjectFingerprint(project)
	m.config.projects[project.ID] = m.nextConfigRevisionLocked()
	m.resetLocked(func(k ModelStickyKey) bool { return m.routes[k].projectID == project.ID })
}
func (m *ModelStickyManager) PublishSystemStrategy(strategy string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.systemStrategy = strategy
	m.config.systemChanged = m.nextConfigRevisionLocked()
	m.resetLocked(func(k ModelStickyKey) bool { return m.routes[k].systemInherited })
}

// PinnedChannelID deliberately excludes unvalidated manual/cold targets: they
// cannot claim the quota backpressure privilege of an existing healthy session.
func (m *ModelStickyManager) PinnedChannelID(key ModelStickyKey) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.routes[key]; r != nil {
		return r.pin
	}
	return 0
}

// RoutingTargetIDs reads quota privilege and target exclusion identity atomically.
// An unvalidated manual target is eligible for exclusion, not sticky privilege.
func (m *ModelStickyManager) RoutingTargetIDs(key ModelStickyKey) (pin, target int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.routes[key]; r != nil {
		target = r.preferred
		if target == 0 {
			target = r.pin
		}
		return r.pin, target
	}
	return 0, 0
}
