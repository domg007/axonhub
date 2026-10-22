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
	epoch           uint64
	pin             int
	probe           bool
	probeChannel    int
	projectID       int
	preferred       int
	notify          chan struct{}
	keyName         string
	candidates      []ModelStickyCandidate
	attempted       map[int]bool
	health          map[int]ModelStickyHealth
	systemInherited bool
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

var ErrModelStickyNoCandidates = errors.New("no available model sticky channel remains")

var ErrModelStickyRequestConflict = errors.New("fixed channel is not eligible for this request; routing state was preserved")
var ErrModelStickySettingsChanged = errors.New("model sticky settings changed during request")

func (e *ModelStickyUnavailable) Error() string {
	return fmt.Sprintf("model sticky routing temporarily unavailable; next recovery in %s", e.RetryAfter.Round(time.Second))
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
		if len(candidates) == 0 {
			m.mu.Unlock()
			return nil, ErrModelStickyNoCandidates
		}
		r := m.routes[key]
		if expected != nil && r != expected {
			m.mu.Unlock()
			return nil, ErrModelStickySettingsChanged
		}
		if r == nil {
			r = newModelStickyRoute()
			m.routes[key] = r
		}
		expected = r
		if a, ok := ctx.Value(modelStickyAdmissionContextKey{}).(*ModelStickyAdmission); ok && a.QuotaExcludedChannelID != 0 && (r.pin == a.QuotaExcludedChannelID || r.preferred == a.QuotaExcludedChannelID) {
			id := a.QuotaExcludedChannelID
			r.attempted[id] = true
			if r.pin == id {
				r.pin = 0
			}
			if r.preferred == id {
				r.preferred = 0
			}
			r.advance()
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
		// Rejected request-specific candidates must not erase target identity: a
		// later real channel removal still needs to find and retire that target.
		r.candidates = append([]ModelStickyCandidate(nil), candidates...)
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
		return errors.New("routing state no longer exists")
	}
	if channelID != 0 {
		found := false
		for _, c := range r.candidates {
			if c.ID == channelID && len(m.availableCandidatesLocked([]ModelStickyCandidate{c})) > 0 {
				found = true
				break
			}
		}
		if !found {
			return errors.New("channel is not in the last authorized candidate set")
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
