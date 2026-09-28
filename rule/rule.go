// Package rule defines inhibition and silence rules and their matching
// semantics against a snapshot in time.
package rule

import "time"

// SilenceRule mutes alerts whose labels contain every Equal pair while
// the rule is active over [StartsAt, EndsAt).
type SilenceRule struct {
	ID       string
	Equal    map[string]string
	StartsAt time.Time
	EndsAt   time.Time
}

// InhibitRule suppresses target alerts matching TargetEqual whenever at
// least one active source alert matches SourceEqual, and for each name in
// Equal the source and target carry the same label value.
type InhibitRule struct {
	ID          string
	SourceEqual map[string]string
	TargetEqual map[string]string
	Equal       []string
}

// Matchers is an immutable snapshot of all routing rules. Reloading rules
// means publishing a new *Matchers; old snapshots remain valid for events
// already being evaluated, so conclusions never change mid-processing.
type Matchers struct {
	silences []SilenceRule
	inhibits []InhibitRule
}

// NewMatchers builds an immutable rule snapshot. The input slices are
// copied so later caller mutation cannot affect the snapshot.
func NewMatchers(silences []SilenceRule, inhibits []InhibitRule) *Matchers {
	m := &Matchers{
		silences: make([]SilenceRule, len(silences)),
		inhibits: make([]InhibitRule, len(inhibits)),
	}
	copy(m.silences, silences)
	copy(m.inhibits, inhibits)
	return m
}

// labelsContain reports whether labels hold every key/value in want.
func labelsContain(labels map[string]string, want map[string]string) bool {
	for k, v := range want {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// MatchSilence returns the ID of the first active silence rule matching
// labels at time at ("" when none applies).
func (m *Matchers) MatchSilence(labels map[string]string, at time.Time) string {
	for i := range m.silences {
		r := &m.silences[i]
		if at.Before(r.StartsAt) || !at.Before(r.EndsAt) {
			continue
		}
		if labelsContain(labels, r.Equal) {
			return r.ID
		}
	}
	return ""
}

// MatchInhibit returns the ID of the first inhibition rule whose target
// selector matches labels and for which sourceLabels provides a matching
// active source ("" when nothing inhibits). The caller passes candidate
// source labels (the alert under evaluation is excluded by the caller so
// an alert can never inhibit itself).
func (m *Matchers) MatchInhibit(labels map[string]string, sourceLabels []map[string]string) string {
	for i := range m.inhibits {
		r := &m.inhibits[i]
		if !labelsContain(labels, r.TargetEqual) {
			continue
		}
		for _, src := range sourceLabels {
			if !labelsContain(src, r.SourceEqual) {
				continue
			}
			ok := true
			for _, name := range r.Equal {
				if src[name] != labels[name] {
					ok = false
					break
				}
			}
			if ok {
				return r.ID
			}
		}
	}
	return ""
}

// Empty is a convenience snapshot containing no rules.
var Empty = NewMatchers(nil, nil)
