package alertrouter

import "time"

// InhibitRule 抑制规则：当存在一条匹配 Source 的、处于触发状态的其他告警时，
// 匹配 Target 的告警被抑制（不通知，状态照常跟踪）。
type InhibitRule struct {
	Name   string
	Source LabelSet
	Target LabelSet
	// Equal 要求 source/target 告警在这些标签上取值相等才生效。
	Equal []string
}

// SilenceRule 静默规则：命中的告警不通知，但状态照常跟踪。
type SilenceRule struct {
	Name  string
	Match LabelSet
}

// RuleSet 是一次加载的全部规则，快照语义：重载整体替换。
type RuleSet struct {
	Inhibits []InhibitRule
	Silences []SilenceRule
}

type suppressionVerdict struct {
	inhibited   bool
	silenced    bool
	inhibitRule string
	silenceRule string
}

// evaluate 判定目标事件是否被静默/抑制。
// 优先级：静默 > 抑制（同时命中时按静默上报），二者都只影响通知、不影响状态跟踪。
// firing 是当前处于触发状态的告警快照，用于查找抑制源；目标自身不会抑制自己。
func (rs RuleSet) evaluate(target AlertEvent, firing []AlertEvent, now time.Time) suppressionVerdict {
	_ = now // 当前规则不含时间窗；保留参数便于扩展
	v := suppressionVerdict{}
	for _, s := range rs.Silences {
		if labelsMatch(target.Labels, s.Match) {
			v.silenced = true
			v.silenceRule = s.Name
			return v // 静默优先，直接返回
		}
	}
	for _, r := range rs.Inhibits {
		if !labelsMatch(target.Labels, r.Target) {
			continue
		}
		for _, src := range firing {
			if src.Fingerprint == target.Fingerprint {
				continue // 自己不能抑制自己
			}
			if !labelsMatch(src.Labels, r.Source) {
				continue
			}
			if !equalAgree(src.Labels, target.Labels, r.Equal) {
				continue
			}
			v.inhibited = true
			v.inhibitRule = r.Name
			return v
		}
	}
	return v
}

// labelsMatch 要求 matchers 中的每个标签都在 labels 中存在且取值完全相等。
// matchers 为空集时不匹配任何告警（避免一条空规则静默全部）。
func labelsMatch(labels, matchers LabelSet) bool {
	if len(matchers) == 0 {
		return false
	}
	for k, val := range matchers {
		if labels[k] != val {
			return false
		}
	}
	return true
}

// equalAgree 校验两条告警在给定标签名上的取值一致。
func equalAgree(a, b LabelSet, names []string) bool {
	for _, n := range names {
		if a[n] != b[n] {
			return false
		}
	}
	return true
}
