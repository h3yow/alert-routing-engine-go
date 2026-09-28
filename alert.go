package alertrouter

import "strings"

// EventType 标识告警事件是触发还是恢复。
type EventType int

const (
	// EventFiring 告警触发。
	EventFiring EventType = iota
	// EventResolved 告警恢复。
	EventResolved
)

// LabelSet 是告警的标签集合，用于分组、去重与规则匹配。
type LabelSet map[string]string

// AlertEvent 是进入引擎的一条事件。
type AlertEvent struct {
	// Fingerprint 是同一条告警的稳定身份，重复事件指纹相同。
	Fingerprint string
	Labels      LabelSet
	Status      EventType
	// StartsAt / EndsAt 由调用方通过注入时钟生成，毫秒时间戳。
	StartsAt int64
	EndsAt   int64
}

// GroupKey 按配置的分组标签（有序）计算分组键。
// 未配置分组标签时，整条告警各自成组（键为指纹由调用方决定，这里返回空串）。
// 缺失的分组标签取值为空串，保证同一标签集合总是得到相同键。
func (e AlertEvent) GroupKey(groupLabels []string) string {
	var b strings.Builder
	for i, name := range groupLabels {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(e.Labels[name])
	}
	return b.String()
}
