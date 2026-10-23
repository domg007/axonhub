package biz

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/objects"
)

// Configuration publication fences old work, but is not an instruction to lose
// a healthy pin or the failure round. Fresh resolution supplies authorization.
type modelStickyEntityState struct {
	revision    uint64
	updatedAt   time.Time
	fingerprint string
	strategy    string
	name        string
	enabled     bool
}

func ModelStickyModelFingerprint(entity *ent.Model) string {
	data, _ := json.Marshal(struct {
		ID                                      int
		DeletedAt                               int
		Name, Developer, Type, Status, Settings string
	}{
		entity.ID, entity.DeletedAt, entity.ModelID, entity.Developer, string(entity.Type), string(entity.Status), modelStickyModelFingerprint(entity.Settings),
	})
	return string(data)
}
func ModelStickyInheritanceFingerprint(settings *SystemModelSettings) string {
	data, _ := json.Marshal(settings.DeveloperSettings)
	return string(data)
}
func modelStickyStrategy(settings *objects.ModelSettings) string {
	if settings == nil {
		return objects.RoutingPolicyDefault
	}
	return objects.NormalizeRoutingPolicyValue(settings.LoadBalancerStrategy)
}
func modelStickyKeyStrategy(key *ent.APIKey) string {
	if p := key.GetActiveProfile(); p != nil && p.LoadBalanceStrategy != nil {
		return objects.NormalizeRoutingPolicyValue(*p.LoadBalanceStrategy)
	}
	return objects.RoutingPolicyDefault
}
func modelStickyEffectiveStrategy(keyStrategy, modelStrategy, systemStrategy string) string {
	for _, s := range []string{keyStrategy, modelStrategy, systemStrategy} {
		if s != "" && s != objects.RoutingPolicyDefault && objects.IsValidLoadBalancerStrategy(s) {
			return s
		}
	}
	return LoadBalancerStrategyAdaptive
}

// Retiring a target is shared by real configuration removal and quota exclusion.
// Neither reason is an upstream failure and neither grows a cooldown.
func (r *modelStickyRoute) retireTarget(id int, attempted bool) bool {
	changed := r.pin == id || r.preferred == id || (r.probe && r.probeChannel == id)
	if r.pin == id {
		r.pin = 0
	}
	if r.preferred == id {
		r.preferred = 0
	}
	if attempted {
		r.attempted[id] = true
	}
	return changed
}
func (m *ModelStickyManager) reconcileCandidatesLocked(r *modelStickyRoute, candidates []ModelStickyCandidate) {
	allowed := make(map[int]ModelStickyCandidate, len(candidates))
	for _, c := range candidates {
		allowed[c.ID] = c
	}
	changed := false
	for _, id := range []int{r.pin, r.preferred, r.probeChannel} {
		if id == 0 {
			continue
		}
		current, ok := allowed[id]
		if !ok {
			changed = r.retireTarget(id, false) || changed
			continue
		}
		for _, old := range r.candidates {
			if old.ID == id && !reflect.DeepEqual(old.Models, current.Models) {
				changed = true
				break
			}
		}
	}
	for id := range r.health {
		if _, ok := allowed[id]; !ok {
			delete(r.health, id)
		}
	}
	for id := range r.attempted {
		if _, ok := allowed[id]; !ok {
			delete(r.attempted, id)
		}
	}
	if changed {
		r.advance()
	}
	r.candidates = append([]ModelStickyCandidate{}, candidates...)
}
func (m *ModelStickyManager) invalidateMatchingLocked(match func(ModelStickyKey, *modelStickyRoute) bool) {
	for key, r := range m.routes {
		if match(key, r) {
			r.advance()
			r.validationPending = true
		}
	}
}

func (m *ModelStickyManager) PublishModel(entity *ent.Model, id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.entities == nil {
		m.config.entities = make(map[int]modelStickyEntityState)
	}
	next := modelStickyEntityState{fingerprint: "deleted-model"}
	if entity != nil {
		next.updatedAt = entity.UpdatedAt
		next.fingerprint = ModelStickyModelFingerprint(entity)
		next.name = entity.ModelID
		next.enabled = entity.Status == model.StatusEnabled && entity.DeletedAt == 0
		next.strategy = modelStickyStrategy(entity.Settings)
	}
	old, known := m.config.entities[id]
	if known && !next.updatedAt.IsZero() && old.updatedAt.After(next.updatedAt) {
		return
	}
	if entity == nil {
		next.name = old.name
		next.strategy = old.strategy
	}
	if known && old.fingerprint == next.fingerprint {
		if next.updatedAt.After(old.updatedAt) {
			old.updatedAt = next.updatedAt
			m.config.entities[id] = old
		}
		return
	}
	next.revision = m.nextConfigRevisionLocked()
	m.config.entities[id] = next
	for key, r := range m.routes {
		// Production routes always carry entity identity. Name fallback exists only
		// for legacy process-local callers without resolution metadata.
		if r.modelEntityID != id && !(r.modelEntityID == 0 && next.name != "" && key.Model == next.name) {
			continue
		}
		r.modelStrategy = next.strategy
		if r.modelEntityID != 0 {
			r.systemInherited = modelStickyInheritsSystem(r.keyStrategy, next.strategy)
		}
		if !next.enabled || key.Model != next.name ||
			(r.modelEntityID != 0 && modelStickyEffectiveStrategy(r.keyStrategy, next.strategy, m.systemStrategyLocked(r)) != LoadBalancerStrategyModelSticky) {
			r.advance()
			delete(m.routes, key)
		} else {
			r.advance()
			r.validationPending = true
		}
	}
}
func (m *ModelStickyManager) PublishInheritance(fingerprint string) {
	m.publishInheritance(fingerprint, time.Time{})
}
func (m *ModelStickyManager) publishInheritance(fingerprint string, updatedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !updatedAt.IsZero() && m.config.inheritanceUpdatedAt.After(updatedAt) {
		return
	}
	if updatedAt.After(m.config.inheritanceUpdatedAt) {
		m.config.inheritanceUpdatedAt = updatedAt
	}
	if m.config.inheritanceKnown && m.config.inheritanceFingerprint == fingerprint {
		return
	}
	m.config.inheritanceKnown = true
	m.config.inheritanceFingerprint = fingerprint
	m.config.inheritanceChanged = m.nextConfigRevisionLocked()
	m.invalidateMatchingLocked(func(ModelStickyKey, *modelStickyRoute) bool { return true })
}

// ReconcileResolved is also used when all request-specific candidates were
// filtered out: target retirement must not depend on a successful Acquire.
func (m *ModelStickyManager) ReconcileResolved(ctx context.Context, key ModelStickyKey, a *ModelStickyAdmission) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx = WithModelStickyAdmission(ctx, a)
	if !m.admissionValidLocked(ctx, key) {
		return ErrModelStickySettingsChanged
	}
	r := m.routes[key]
	if r == nil {
		return nil
	}
	if a.ModelEntityID != 0 && r.modelEntityID != 0 && a.ModelEntityID != r.modelEntityID {
		r.advance()
		delete(m.routes, key)
		return nil
	}
	if a.StructuralCandidates != nil {
		m.reconcileCandidatesLocked(r, m.availableCandidatesLocked(a.StructuralCandidates))
		r.validationPending = false
	}
	return nil
}

func (m *ModelStickyManager) systemStrategyLocked(r *modelStickyRoute) string {
	if m.config.systemStrategy != "" {
		return m.config.systemStrategy
	}
	return r.systemStrategy
}
func modelStickyInheritsSystem(keyStrategy, modelStrategy string) bool {
	explicit := func(s string) bool {
		return s != "" && s != objects.RoutingPolicyDefault && objects.IsValidLoadBalancerStrategy(s)
	}
	return !explicit(keyStrategy) && !explicit(modelStrategy)
}

// A failed maintenance read is not evidence that the whole route is invalid.
// Fence old requests and let fresh authorized resolution recover without losing
// successful pins, cooldowns or unfinished rounds.
func (m *ModelStickyManager) fenceKeys(ids []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.keys == nil {
		m.config.keys = make(map[int]uint64)
	}
	for _, id := range ids {
		m.config.keys[id] = m.nextConfigRevisionLocked()
		delete(m.config.keyFingerprints, id)
		m.invalidateMatchingLocked(func(k ModelStickyKey, _ *modelStickyRoute) bool { return k.APIKeyID == id })
	}
}
func (m *ModelStickyManager) fenceProject(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.projects == nil {
		m.config.projects = make(map[int]uint64)
	}
	m.config.projects[id] = m.nextConfigRevisionLocked()
	delete(m.config.projectFingerprints, id)
	m.invalidateMatchingLocked(func(_ ModelStickyKey, r *modelStickyRoute) bool { return r.projectID == id })
}
func (m *ModelStickyManager) fenceSystemInherited() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.systemChanged = m.nextConfigRevisionLocked()
	m.config.systemStrategy = ""
	m.invalidateMatchingLocked(func(_ ModelStickyKey, r *modelStickyRoute) bool { return r.systemInherited })
}

// A validated request can establish the initial effective fingerprints before
// the first settings save in this process. Learning facts is not a change or a
// revision: a later identical/rename-only publication must not restart a probe.
func (m *ModelStickyManager) rememberAdmissionLocked(key ModelStickyKey, a *ModelStickyAdmission) {
	if a.ModelEntityID == 0 {
		return
	}
	if m.config.entities == nil {
		m.config.entities = make(map[int]modelStickyEntityState)
	}
	if _, known := m.config.entities[a.ModelEntityID]; !known {
		m.config.entities[a.ModelEntityID] = modelStickyEntityState{fingerprint: a.ModelFingerprint, name: key.Model, strategy: a.ModelStrategy, enabled: true}
	}
	if m.config.keyFingerprints == nil {
		m.config.keyFingerprints = make(map[int]string)
	}
	if _, known := m.config.keyFingerprints[key.APIKeyID]; !known {
		m.config.keyFingerprints[key.APIKeyID] = a.keyFingerprint
	}
	if a.projectID != 0 && a.projectFingerprint != "" {
		if m.config.projectFingerprints == nil {
			m.config.projectFingerprints = make(map[int]string)
		}
		if _, known := m.config.projectFingerprints[a.projectID]; !known {
			m.config.projectFingerprints[a.projectID] = a.projectFingerprint
		}
	}
}
