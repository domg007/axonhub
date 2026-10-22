package biz

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/schema/schematype"
	"github.com/looplj/axonhub/internal/log"
)

type modelStickyChannelState struct {
	updatedAt time.Time
	enabled   bool
	entries   map[string]ChannelModelEntry
}

// PublishChannel is called from committed saves AND actual cache swaps, including
// scheduled automatic model sync. Older cache reloads cannot resurrect a target.
func (m *ModelStickyManager) PublishChannel(entity *ent.Channel) {
	if entity == nil {
		return
	}
	ch := &Channel{Channel: entity}
	state := modelStickyChannelState{updatedAt: entity.UpdatedAt, enabled: entity.Status == channel.StatusEnabled && entity.DeletedAt == 0, entries: ch.GetModelEntries()}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishChannelLocked(entity.ID, state)
}
func (m *ModelStickyManager) publishChannelLocked(id int, state modelStickyChannelState) {
	if m.channels == nil {
		m.channels = make(map[int]modelStickyChannelState)
	}
	if old, ok := m.channels[id]; ok && old.updatedAt.After(state.updatedAt) {
		return
	}
	m.channels[id] = state
	for _, r := range m.routes {
		changed := false
		for _, c := range r.candidates {
			if c.ID != id {
				continue
			}
			available := m.availableCandidatesLocked([]ModelStickyCandidate{c})
			if len(available) == 0 {
				if r.pin == id {
					r.pin = 0
					changed = true
				}
				if r.preferred == id {
					r.preferred = 0
					changed = true
				}
				if r.probeChannel == id {
					changed = true
				}
				r.attempted[id] = true
			} else if len(available[0].Models) != len(c.Models) && (r.pin == id || r.probeChannel == id) {
				// Partial model removal must fence old attempts too, but a legal model
				// alternative on the same channel may keep the successful channel pin.
				changed = true
			}
		}
		if changed {
			r.advance()
		}
		r.candidates = m.availableCandidatesLocked(r.candidates)
	}
}
func (m *ModelStickyManager) ChannelCacheSwapped(old, new []*Channel) {
	for _, ch := range new {
		if ch != nil && ch.Channel != nil {
			m.PublishChannel(ch.Channel)
		}
	}
	present := make(map[int]bool, len(new))
	for _, ch := range new {
		if ch != nil && ch.Channel != nil {
			present[ch.ID] = true
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range old {
		if ch != nil && ch.Channel != nil && !present[ch.ID] {
			m.publishChannelLocked(ch.ID, modelStickyChannelState{updatedAt: ch.UpdatedAt})
		}
	}
}
func (m *ModelStickyManager) availableCandidatesLocked(candidates []ModelStickyCandidate) []ModelStickyCandidate {
	result := make([]ModelStickyCandidate, 0, len(candidates))
	for _, c := range candidates {
		state, known := m.channels[c.ID]
		if !known {
			result = append(result, c)
			continue
		}
		if !state.enabled {
			continue
		}
		if len(c.Models) == 0 {
			result = append(result, c)
			continue
		}
		clone := c
		clone.Models = nil
		for _, entry := range c.Models {
			current, ok := state.entries[entry.RequestModel]
			if ok && current.ActualModel == entry.ActualModel {
				clone.Models = append(clone.Models, entry)
			}
		}
		if len(clone.Models) > 0 {
			result = append(result, clone)
		}
	}
	return result
}
func (m *ModelStickyManager) newLeaseLocked(key ModelStickyKey, r *modelStickyRoute, id int, candidates []ModelStickyCandidate) *ModelStickyLease {
	l := &ModelStickyLease{manager: m, key: key, route: r, epoch: r.epoch, ChannelID: id}
	for _, c := range candidates {
		if c.ID == id {
			l.Models = append([]ChannelModelEntry(nil), c.Models...)
			break
		}
	}
	return l
}

func (svc *ChannelService) publishModelStickyChannelAfterCommit(ctx context.Context, id int) {
	runAfterCommit(ctx, func(ctx context.Context) {
		ch, err := svc.db.Channel.Get(schematype.SkipSoftDelete(ctx), id)
		if err != nil {
			log.Warn(ctx, "failed to read committed sticky channel", log.Cause(err))
			return
		}
		ModelStickyRuntime.PublishChannel(ch)
	})
}
