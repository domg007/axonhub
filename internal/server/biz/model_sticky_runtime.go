package biz

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const LoadBalancerStrategyModelSticky = "model-sticky"

// ModelStickyRuntime is process-local. It deliberately stores no API key secrets.
var ModelStickyRuntime = NewModelStickyManager()

type ModelStickyKey struct {
	APIKeyID int    `json:"apiKeyID"`
	Model    string `json:"model"`
}

type ModelStickyCandidate struct {
	ID     int                 `json:"id"`
	Name   string              `json:"name"`
	Models []ChannelModelEntry `json:"-"`
}

type ModelStickyHealth struct {
	ChannelID int       `json:"channelID"`
	Failures  int       `json:"failures"`
	Until     time.Time `json:"until"`
}

type ModelStickySnapshot struct {
	ModelStickyKey
	KeyName            string                 `json:"keyName"`
	ChannelID          int                    `json:"channelID"`
	Probing            bool                   `json:"probing"`
	PreferredChannelID int                    `json:"preferredChannelID"`
	ProbeChannelID     int                    `json:"probeChannelID"`
	Candidates         []ModelStickyCandidate `json:"candidates"`
	Cooldowns          []ModelStickyHealth    `json:"cooldowns"`
	Attempted          []int                  `json:"attempted"`
}

type modelStickyRoute struct {
	epoch                      uint64
	pin                        int
	probe                      bool
	probeChannel               int
	projectID                  int
	preferred                  int
	notify                     chan struct{}
	keyName                    string
	candidates                 []ModelStickyCandidate
	attempted                  map[int]bool
	health                     map[int]ModelStickyHealth
	systemInherited            bool
	modelEntityID              int
	modelStrategy, keyStrategy string
	systemStrategy             string
	validationPending          bool
}

type ModelStickyManager struct {
	mu       sync.Mutex
	routes   map[ModelStickyKey]*modelStickyRoute
	now      func() time.Time
	config   modelStickyConfigState
	channels map[int]modelStickyChannelState
}

func NewModelStickyManager() *ModelStickyManager {
	return &ModelStickyManager{routes: make(map[ModelStickyKey]*modelStickyRoute), now: time.Now}
}

// ModelStickyUnavailable never causes an early probe of a cooling channel.
// RetryAfter is informational; callers should still respect their own deadlines.
type ModelStickyUnavailable struct{ RetryAfter time.Duration }

var ErrModelStickyNoCandidates = errors.New("【新增策略：模型固定渠道】当前没有可用候选渠道，请检查模型关联、渠道权限及冷却状态。")

var ErrModelStickyRequestConflict = errors.New("【新增策略：模型固定渠道】固定渠道不符合本次请求条件；原固定状态已保留，本次未自动切换渠道。")
var ErrModelStickySettingsChanged = errors.New("【新增策略：模型固定渠道】路由配置已在请求期间变更，请重新发送请求。")

func (e *ModelStickyUnavailable) Error() string {
	return fmt.Sprintf("【新增策略：模型固定渠道】候选渠道暂时不可用，请在 %d 秒后重试。", max(1, int((e.RetryAfter+time.Second-1)/time.Second)))
}

// ModelStickyLease fences late completions using route identity and epoch.
type ModelStickyLease struct {
	manager   *ModelStickyManager
	key       ModelStickyKey
	route     *modelStickyRoute
	epoch     uint64
	ChannelID int
	Models    []ChannelModelEntry
}

func newModelStickyRoute() *modelStickyRoute {
	return &modelStickyRoute{systemInherited: true, notify: make(chan struct{}), attempted: make(map[int]bool), health: make(map[int]ModelStickyHealth)}
}

func (r *modelStickyRoute) advance() {
	r.epoch++
	close(r.notify)
	r.notify = make(chan struct{})
	r.probe = false
	r.probeChannel = 0
}

// Acquire uses the caller's freshly authorized, ordered candidates. tried is
// request-local and must never be cleared, even when a shared round completes.
func (m *ModelStickyManager) Acquire(ctx context.Context, key ModelStickyKey, keyName string, candidates []ModelStickyCandidate, tried map[int]bool, freshRequest bool, previous ...*ModelStickyLease) (*ModelStickyLease, error) {
	var expected *modelStickyRoute
	if len(previous) > 0 && previous[0] != nil {
		expected = previous[0].route
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		if !m.admissionValidLocked(ctx, key) {
			m.mu.Unlock()
			return nil, ErrModelStickySettingsChanged
		}
		candidates = m.availableCandidatesLocked(candidates)
		r := m.routes[key]
		if expected != nil && r != expected {
			m.mu.Unlock()
			return nil, ErrModelStickySettingsChanged
		}
		if r == nil {
			r = newModelStickyRoute()
			m.routes[key] = r
		}
		if a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission); ok {
			if a.ModelEntityID != 0 && r.modelEntityID != 0 && a.ModelEntityID != r.modelEntityID {
				if expected != nil {
					m.mu.Unlock()
					return nil, ErrModelStickySettingsChanged
				}
				r.advance()
				r = newModelStickyRoute()
				m.routes[key] = r
			}
			if a.StructuralCandidates != nil {
				m.reconcileCandidatesLocked(r, m.availableCandidatesLocked(a.StructuralCandidates))
				r.validationPending = false
			}
			m.rememberAdmissionLocked(key, a)
			r.modelEntityID = a.ModelEntityID
			r.modelStrategy = a.ModelStrategy
			r.keyStrategy = a.keyStrategy
			r.systemStrategy = a.systemStrategy
		}
		expected = r
		if a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission); ok && a.QuotaExcludedChannelID != 0 && (r.pin == a.QuotaExcludedChannelID || r.preferred == a.QuotaExcludedChannelID) {
			if r.retireTarget(a.QuotaExcludedChannelID, true) {
				r.advance()
			}
		}
		if a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission); ok {
			r.systemInherited = a.inherited
			r.projectID = a.projectID
		}
		r.keyName = keyName
		allowed := make(map[int]bool, len(candidates))
		for _, c := range candidates {
			allowed[c.ID] = true
		}
		if r.preferred != 0 && !allowed[r.preferred] {
			m.mu.Unlock()
			return nil, ErrModelStickyRequestConflict
		}
		if r.pin != 0 && !allowed[r.pin] {
			m.mu.Unlock()
			return nil, ErrModelStickyRequestConflict
		}
		if len(candidates) == 0 {
			m.mu.Unlock()
			return nil, ErrModelStickyNoCandidates
		}
		// Rejected request-specific candidates must not erase target identity: a
		// later real channel removal still needs to find and retire that target.
		if a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission); !ok || a.StructuralCandidates == nil {
			r.candidates = append([]ModelStickyCandidate(nil), candidates...)
		}
		// A concurrent request may have pinned a channel this request already
		// attempted. Never create a second probe alongside that healthy pin.
		if r.pin != 0 && tried[r.pin] {
			m.mu.Unlock()
			return nil, &ModelStickyUnavailable{}
		}
		if r.pin != 0 && !tried[r.pin] {
			lease := m.newLeaseLocked(key, r, r.pin, candidates)
			m.mu.Unlock()
			return lease, nil
		}
		if r.probe {
			wait := r.notify
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-wait:
				continue
			}
		}
		now := m.now()
		id := 0
		untried := false
		var earliest time.Time
		var earliestUntried time.Time
		for _, c := range candidates {
			if !r.attempted[c.ID] {
				untried = true
			}
			until := r.health[c.ID].Until
			if !r.attempted[c.ID] && !tried[c.ID] && until.After(now) && (earliestUntried.IsZero() || until.Before(earliestUntried)) {
				earliestUntried = until
			}
			if until.After(now) && (earliest.IsZero() || until.Before(earliest)) {
				earliest = until
			}
			if tried[c.ID] || r.attempted[c.ID] || until.After(now) {
				continue
			}
			if id == 0 || c.ID == r.preferred {
				id = c.ID
			}
		}
		// Do not wrap while any authorized candidate is still unattempted. In
		// particular an expired a must not steal the turn of a cooling y/z.
		if id == 0 && !untried && freshRequest {
			for _, c := range candidates {
				if tried[c.ID] || r.health[c.ID].Until.After(now) {
					continue
				}
				id = c.ID
				r.attempted = make(map[int]bool)
				break
			}
		}
		if id == 0 {
			if untried {
				earliest = earliestUntried
			}
			delay := time.Duration(0)
			if !earliest.IsZero() {
				delay = earliest.Sub(now)
			}
			m.mu.Unlock()
			return nil, &ModelStickyUnavailable{RetryAfter: delay}
		}
		r.probe = true
		r.probeChannel = id
		lease := m.newLeaseLocked(key, r, id, candidates)
		m.mu.Unlock()
		return lease, nil
	}
}

func (l *ModelStickyLease) valid() bool {
	return l.manager.routes[l.key] == l.route && l.route.epoch == l.epoch
}

// Active checks both administrative identity and the current failure epoch.
func (l *ModelStickyLease) Active() bool {
	if l == nil {
		return false
	}
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	return l.valid()
}

func (l *ModelStickyLease) SetSystemInherited(inherited bool) {
	if l == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.valid() {
		l.route.systemInherited = inherited
	}
}
func (m *ModelStickyManager) ResetSystemInherited() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.systemChanged = m.nextConfigRevisionLocked()
	m.resetLocked(func(k ModelStickyKey) bool { return m.routes[k].systemInherited })
}
func (m *ModelStickyManager) HasRoutes() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.routes) > 0
}

func (l *ModelStickyLease) Success() {
	if l == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if !l.valid() {
		return
	}
	r := l.route
	r.pin = l.ChannelID
	r.preferred = 0
	delete(r.health, l.ChannelID)
	r.attempted = make(map[int]bool)
	// Keep the same epoch: other healthy concurrent leases remain valid. A
	// failure or reset advances the epoch and invalidates all these leases.
	if r.probe {
		r.probe = false
		r.probeChannel = 0
		close(r.notify)
		r.notify = make(chan struct{})
	}
}

func (l *ModelStickyLease) Failure() {
	if l == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if !l.valid() {
		return
	}
	r := l.route
	h := r.health[l.ChannelID]
	h.ChannelID = l.ChannelID
	h.Failures++
	delay := 30 * time.Second
	for i := 1; i < h.Failures && delay < 10*time.Minute; i++ {
		delay *= 2
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	h.Until = m.now().Add(delay)
	r.health[l.ChannelID] = h
	r.attempted[l.ChannelID] = true
	r.pin = 0
	if r.preferred == l.ChannelID {
		r.preferred = 0
	}
	r.advance()
}

func (l *ModelStickyLease) Abandon() {
	if l == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.valid() && l.route.probe {
		l.route.advance()
	}
}

func (m *ModelStickyManager) resetMatching(match func(ModelStickyKey) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetLocked(match)
}

func (m *ModelStickyManager) resetLocked(match func(ModelStickyKey) bool) {
	for key, r := range m.routes {
		if match(key) {
			r.advance()
			delete(m.routes, key)
		}
	}
}
func (m *ModelStickyManager) ResetAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.allChanged = m.nextConfigRevisionLocked()
	m.resetLocked(func(ModelStickyKey) bool { return true })
}
func (m *ModelStickyManager) ResetKey(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetKeyLocked(id)
}
func (m *ModelStickyManager) resetKeyLocked(id int) {
	if m.config.keys == nil {
		m.config.keys = make(map[int]uint64)
	}
	m.config.keys[id] = m.nextConfigRevisionLocked()
	m.resetLocked(func(k ModelStickyKey) bool { return k.APIKeyID == id })
}
func (m *ModelStickyManager) ResetModel(model string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.models == nil {
		m.config.models = make(map[string]uint64)
	}
	m.config.models[model] = m.nextConfigRevisionLocked()
	m.resetLocked(func(k ModelStickyKey) bool { return k.Model == model })
}

// Control changes runtime state, never authorization. Acquire always checks
// actual request candidates again, even after an administrator chooses a pin.
func (m *ModelStickyManager) Control(key ModelStickyKey, channelID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.routes[key]
	if r == nil {
		return errors.New("【新增策略：模型固定渠道】固定状态已不存在，请刷新列表后重试。")
	}
	if channelID != 0 {
		if r.validationPending {
			return errors.New("【新增策略：模型固定渠道】授权候选正在等待最新请求重新确认，请先发起模型请求后刷新列表。")
		}
		found := false
		for _, c := range r.candidates {
			if c.ID == channelID && len(m.availableCandidatesLocked([]ModelStickyCandidate{c})) > 0 {
				found = true
				break
			}
		}
		if !found {
			return errors.New("【新增策略：模型固定渠道】所选渠道不在该 Key 与模型最近确认的授权候选范围内，请刷新后重新选择。")
		}
	}
	if m.config.controls == nil {
		m.config.controls = make(map[ModelStickyKey]uint64)
	}
	m.config.controls[key] = m.nextConfigRevisionLocked()
	// Replace identity: waking old waiters must not authorize them to reenter.
	next := newModelStickyRoute()
	next.systemInherited = r.systemInherited
	next.projectID = r.projectID
	next.modelEntityID = r.modelEntityID
	next.modelStrategy = r.modelStrategy
	next.keyStrategy = r.keyStrategy
	next.systemStrategy = r.systemStrategy
	next.validationPending = r.validationPending
	next.keyName = r.keyName
	next.candidates = append([]ModelStickyCandidate(nil), r.candidates...)
	next.preferred = channelID
	if channelID != 0 {
		for id, h := range r.health {
			if id != channelID {
				next.health[id] = h
			}
		}
	}
	r.advance()
	m.routes[key] = next
	return nil
}

func (m *ModelStickyManager) Snapshots() []ModelStickySnapshot {
	m.mu.Lock()
	rows := make([]ModelStickySnapshot, 0, len(m.routes))
	for key, r := range m.routes {
		row := ModelStickySnapshot{ModelStickyKey: key, KeyName: r.keyName, ChannelID: r.pin, Probing: r.probe, PreferredChannelID: r.preferred, ProbeChannelID: r.probeChannel,
			Candidates: append([]ModelStickyCandidate{}, r.candidates...), Cooldowns: []ModelStickyHealth{}, Attempted: []int{}}
		for _, h := range r.health {
			row.Cooldowns = append(row.Cooldowns, h)
		}
		for id := range r.attempted {
			row.Attempted = append(row.Attempted, id)
		}
		rows = append(rows, row)
	}
	m.mu.Unlock()
	for i := range rows {
		row := &rows[i]
		sort.Ints(row.Attempted)
		sort.Slice(row.Cooldowns, func(i, j int) bool { return row.Cooldowns[i].ChannelID < row.Cooldowns[j].ChannelID })
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].APIKeyID != rows[j].APIKeyID {
			return rows[i].APIKeyID < rows[j].APIKeyID
		}
		return rows[i].Model < rows[j].Model
	})
	return rows
}
