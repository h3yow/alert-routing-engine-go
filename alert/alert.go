// Package alert defines the alert event domain model.
package alert

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// Status is the lifecycle status of an alert fingerprint.
type Status int

const (
	// StatePending means a firing alert that is currently tracked as active.
	StatePending Status = iota
	// StateResolved means the alert has seen a matching recovery event.
	StateResolved
)

func (s Status) String() string { return statusNames[s] }

var statusNames = [...]string{"pending", "resolved"}

// EventKind distinguishes trigger and recovery events.
type EventKind int

const (
	KindFiring EventKind = iota
	KindResolved
)

func (k EventKind) String() string { return kindNames[k] }

var kindNames = [...]string{"firing", "resolved"}

// Outcome categorizes the engine's conclusion for one incoming event.
type Outcome int

const (
	// OutcomeNotified: new firing alert, a notification was produced.
	OutcomeNotified Outcome = iota
	// OutcomeSuppressed: inhibited by an active source alert.
	OutcomeSuppressed
	// OutcomeSilenced: matched an active silence rule.
	OutcomeSilenced
	// OutcomeDuplicate: repeat event, must not produce a notification.
	OutcomeDuplicate
	// OutcomeOrphanResolve: recovery with no prior firing on record.
	OutcomeOrphanResolve
	// OutcomeOutOfOrder: firing arriving after the alert already resolved.
	OutcomeOutOfOrder
	// OutcomeResolvedNotification: resolution notification produced.
	OutcomeResolvedNotification
	// OutcomeResolvedNoNotification: resolved while no firing notification
	// was ever sent (muted for its whole active life); state tracked only.
	OutcomeResolvedNoNotification
)

func (o Outcome) String() string { return outcomeNames[o] }

var outcomeNames = [...]string{
	"notified",
	"suppressed",
	"silenced",
	"duplicate",
	"orphan_resolve",
	"out_of_order",
	"resolved_notification",
	"resolved_no_notification",
}

// Alert is a single trigger or recovery event from a source.
type Alert struct {
	// Fingerprint uniquely identifies the alerting condition the event
	// belongs to; trigger and recovery for the same condition share it.
	Fingerprint string
	Labels      map[string]string
	StartsAt    time.Time
	EndsAt      time.Time
	Kind        EventKind
}

// Firing reports whether the event is a trigger event.
func (a Alert) Firing() bool { return a.Kind == KindFiring }

// Resolved reports whether the event is a recovery event.
func (a Alert) Resolved() bool { return a.Kind == KindResolved }

// GroupKey computes a deterministic routing key for a label set.
// Labels are taken in the sorted order of groupBy names so the result
// does not depend on map iteration. An empty groupBy yields the empty
// key, placing every alert in a single global group.
func GroupKey(labels map[string]string, groupBy []string) string {
	if len(groupBy) == 0 {
		return ""
	}
	names := make([]string, len(groupBy))
	copy(names, groupBy)
	sort.Strings(names)
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(name))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(labels[name]))
	}
	return b.String()
}
