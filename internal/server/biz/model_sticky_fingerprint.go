package biz

import (
	"encoding/json"
	"slices"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
)

// Only effective routing/permission fields invalidate a sticky route. Names,
// inactive profiles, template labels and quota bookkeeping do not change pins.
func modelStickyProfileFingerprint(profile *objects.APIKeyProfile) string {
	p := profile.Clone()
	if p == nil {
		p = &objects.APIKeyProfile{}
	}
	p.Name = ""
	p.TemplateID = nil
	p.TemplateName = ""
	p.Quota = nil
	p.TraceStickyMode = nil
	strategy := objects.RoutingPolicyDefault
	if p.LoadBalanceStrategy != nil {
		strategy = objects.NormalizeRoutingPolicyValue(*p.LoadBalanceStrategy)
	}
	p.LoadBalanceStrategy = &strategy
	p.ChannelIDs = slices.Compact(sortedStickyInts(p.ChannelIDs))
	p.ChannelTags = slices.Compact(sortedStickyStrings(p.ChannelTags))
	p.ModelIDs = slices.Compact(sortedStickyStrings(p.ModelIDs))
	if len(p.ModelMappings) == 0 {
		p.ModelMappings = nil
	}
	p.ChannelTagsMatchMode = p.ChannelTagsMatchMode.OrDefault()
	data, _ := json.Marshal(p)
	return string(data)
}
func sortedStickyInts(values []int) []int {
	result := slices.Clone(values)
	slices.Sort(result)
	if len(result) == 0 {
		return nil
	}
	return result
}
func sortedStickyStrings(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	if len(result) == 0 {
		return nil
	}
	return result
}

func modelStickyKeyFingerprint(key *ent.APIKey) string {
	data, _ := json.Marshal(struct {
		Profile string
		Status  string
		Scopes  []string
		IPs     []string
	}{modelStickyProfileFingerprint(key.GetActiveProfile()), string(key.Status), sortedStickyStrings(key.Scopes), sortedStickyStrings(key.AllowedIps)})
	return string(data)
}
func modelStickyProjectFingerprint(project *ent.Project) string {
	p := project.GetActiveProfile()
	if p == nil {
		return modelStickyProfileFingerprint(nil)
	}
	return modelStickyProfileFingerprint(&objects.APIKeyProfile{ChannelIDs: p.ChannelIDs, ChannelTags: p.ChannelTags, ChannelTagsMatchMode: p.ChannelTagsMatchMode})
}
func modelStickyModelFingerprint(settings *objects.ModelSettings) string {
	var p objects.ModelSettings
	if settings != nil {
		p = *settings
	}
	p.TraceStickyMode = ""
	p.LoadBalancerStrategy = objects.NormalizeRoutingPolicyValue(p.LoadBalancerStrategy)
	if len(p.Associations) == 0 {
		p.Associations = nil
	}
	data, _ := json.Marshal(p)
	return string(data)
}
