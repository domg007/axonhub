package biz

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/objects"
)

// Revisions fence ABA changes even when a route has not yet been created.
// These process-local tombstones are intentionally independent of live routes.
type modelStickyConfigState struct {
	systemUpdatedAt                     time.Time
	keyUpdatedAt                        map[int]time.Time
	projectUpdatedAt                    map[int]time.Time
	inheritanceUpdatedAt                time.Time
	revision, allChanged, systemChanged uint64
	keys                                map[int]uint64
	models                              map[string]uint64
	projects                            map[int]uint64
	controls                            map[ModelStickyKey]uint64
	keyFingerprints                     map[int]string
	projectFingerprints                 map[int]string
	systemStrategy                      string
	entities                            map[int]modelStickyEntityState
	inheritanceChanged                  uint64
	inheritanceKnown                    bool
	inheritanceFingerprint              string
}
type ModelStickyConfigSnapshot struct {
	manager  *ModelStickyManager
	revision uint64
}
type modelStickyConfigContextKey struct{}
type modelStickyAdmissionContextKey struct{}
type ModelStickyAdmission struct {
	QuotaExcludedChannelID                                  int
	ModelEntityID                                           int
	ModelFingerprint, InheritanceFingerprint, ModelStrategy string
	StructuralCandidates                                    []ModelStickyCandidate
	snapshot                                                *ModelStickyConfigSnapshot
	keyFingerprint, projectFingerprint, systemStrategy      string
	projectID                                               int
	inherited                                               bool
	keyStrategy                                             string
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
	a := &ModelStickyAdmission{keyStrategy: modelStickyKeyStrategy(key), snapshot: snapshot, inherited: inherited, systemStrategy: strategy, keyFingerprint: modelStickyKeyFingerprint(key), projectID: key.ProjectID}
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
	if c := m.config.entities[a.ModelEntityID]; a.ModelEntityID != 0 && (c.revision > rev || (c.fingerprint != "" && (c.fingerprint != a.ModelFingerprint || !c.enabled))) {
		return false
	}
	if m.config.inheritanceChanged > rev || (a.ModelEntityID != 0 && m.config.inheritanceKnown && m.config.inheritanceFingerprint != a.InheritanceFingerprint) {
		return false
	}
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
	if m.config.keyUpdatedAt == nil {
		m.config.keyUpdatedAt = make(map[int]time.Time)
	}
	if !key.UpdatedAt.IsZero() && m.config.keyUpdatedAt[key.ID].After(key.UpdatedAt) {
		return
	}
	if key.UpdatedAt.After(m.config.keyUpdatedAt[key.ID]) {
		m.config.keyUpdatedAt[key.ID] = key.UpdatedAt
	}
	if m.config.keyFingerprints == nil {
		m.config.keyFingerprints = make(map[int]string)
	}
	fingerprint := modelStickyKeyFingerprint(key)
	if m.config.keyFingerprints[key.ID] == fingerprint {
		return
	}
	m.config.keyFingerprints[key.ID] = fingerprint
	if m.config.keys == nil {
		m.config.keys = make(map[int]uint64)
	}
	m.config.keys[key.ID] = m.nextConfigRevisionLocked()
	for k, r := range m.routes {
		if k.APIKeyID != key.ID {
			continue
		}
		strategy := modelStickyKeyStrategy(key)
		r.keyStrategy = strategy
		if r.modelEntityID != 0 {
			r.systemInherited = modelStickyInheritsSystem(strategy, r.modelStrategy)
		}
		if key.Status != apikey.StatusEnabled ||
			((r.modelEntityID != 0 || strategy != objects.RoutingPolicyDefault) &&
				modelStickyEffectiveStrategy(strategy, r.modelStrategy, m.systemStrategyLocked(r)) != LoadBalancerStrategyModelSticky) {
			r.advance()
			delete(m.routes, k)
		} else {
			r.advance()
			r.validationPending = true
		}
	}
}
func (m *ModelStickyManager) PublishProject(project *ent.Project) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.projectUpdatedAt == nil {
		m.config.projectUpdatedAt = make(map[int]time.Time)
	}
	if !project.UpdatedAt.IsZero() && m.config.projectUpdatedAt[project.ID].After(project.UpdatedAt) {
		return
	}
	if project.UpdatedAt.After(m.config.projectUpdatedAt[project.ID]) {
		m.config.projectUpdatedAt[project.ID] = project.UpdatedAt
	}
	if m.config.projects == nil {
		m.config.projects = make(map[int]uint64)
	}
	if m.config.projectFingerprints == nil {
		m.config.projectFingerprints = make(map[int]string)
	}
	fingerprint := modelStickyProjectFingerprint(project)
	if m.config.projectFingerprints[project.ID] == fingerprint {
		return
	}
	m.config.projectFingerprints[project.ID] = fingerprint
	m.config.projects[project.ID] = m.nextConfigRevisionLocked()
	m.invalidateMatchingLocked(func(_ ModelStickyKey, r *modelStickyRoute) bool { return r.projectID == project.ID })
}
func (m *ModelStickyManager) PublishSystemStrategy(strategy string) {
	m.publishSystemStrategy(strategy, time.Time{})
}
func (m *ModelStickyManager) publishSystemStrategy(strategy string, updatedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !updatedAt.IsZero() && m.config.systemUpdatedAt.After(updatedAt) {
		return
	}
	if updatedAt.After(m.config.systemUpdatedAt) {
		m.config.systemUpdatedAt = updatedAt
	}
	if m.config.systemStrategy == strategy {
		return
	}
	firstPublication := m.config.systemStrategy == ""
	m.config.systemStrategy = strategy
	if !firstPublication {
		m.config.systemChanged = m.nextConfigRevisionLocked()
	}
	// The process may have served requests before the first settings save.
	// Saving that same effective strategy is not a strategy switch.
	m.resetLocked(func(k ModelStickyKey) bool {
		r := m.routes[k]
		return r.systemInherited && (!firstPublication || r.systemStrategy != strategy)
	})
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
