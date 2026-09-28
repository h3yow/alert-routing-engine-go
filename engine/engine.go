// Package engine wires ingestion, state tracking, silence/inhibition
// rules and local notification dispatch into one routing engine.
package engine

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/h3yow/alert-routing-engine-go/alert"
	"github.com/h3yow/alert-routing-engine-go/clock"
	"github.com/h3yow/alert-routing-engine-go/notify"
	"github.com/h3yow/alert-routing-engine-go/rule"
	"github.com/h3yow/alert-routing-engine-go/state"
)

// Config holds engine routing configuration.
type Config struct {
	// GroupBy lists label names used to compute the routing group.
	GroupBy []string
}

// Engine ingests alert events and routes resulting notifications.
//
// State transitions are serialized inside the store's write lock, so any
// number of producers may call Ingest concurrently without torn updates.
// Dispatch inputs (the notification value) are fixed under that lock and
// delivery itself is asynchronous, so no two events can produce duplicate
// messages for one fingerprint. Reload only swaps an atomic rule pointer;
// an event evaluates against the snapshot it captured on entry.
type Engine struct {
	clk      clock.Clock
	store    *state.Store
	disp     *notify.Dispatcher
	rec      *Recorder
	groupBy  []string
	notifSeq uint64

	mu    sync.Mutex
	rules *rule.Matchers
}

// New constructs an engine. nil rules means no silence/inhibition.
func New(clk clock.Clock, channels []notify.Channel, rules *rule.Matchers, cfg Config, rec *Recorder) *Engine {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	if rules == nil {
		rules = rule.Empty
	}
	if rec == nil {
		rec = NewRecorder()
	}
	e := &Engine{
		clk:     clk,
		store:   state.NewStore(),
		disp:    notify.NewDispatcher(clk, channels, notify.DefaultConfig()),
		rec:     rec,
		groupBy: append([]string(nil), cfg.GroupBy...),
		rules:   rules,
	}
	e.disp.SetResultSink(rec.RecordResult)
	return e
}

// NewWithDispatchConfig is New with explicit retry/backoff settings.
func NewWithDispatchConfig(clk clock.Clock, channels []notify.Channel, rules *rule.Matchers, cfg Config, dcfg notify.Config, rec *Recorder) *Engine {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	if rules == nil {
		rules = rule.Empty
	}
	if rec == nil {
		rec = NewRecorder()
	}
	e := &Engine{
		clk:     clk,
		store:   state.NewStore(),
		disp:    notify.NewDispatcher(clk, channels, dcfg),
		rec:     rec,
		groupBy: append([]string(nil), cfg.GroupBy...),
		rules:   rules,
	}
	e.disp.SetResultSink(rec.RecordResult)
	return e
}

func (e *Engine) currentRules() *rule.Matchers {
	e.mu.Lock()
	r := e.rules
	e.mu.Unlock()
	return r
}

// ReplaceChannels atomically swaps notification channels.
func (e *Engine) ReplaceChannels(channels []notify.Channel) {
	e.disp.ReplaceChannels(channels)
}

// Reload atomically replaces the rule snapshot. Events already inside
// Ingest keep their captured snapshot; queued deliveries are neither
// cancelled nor duplicated by the swap. Subsequent events use new rules.
func (e *Engine) Reload(rules *rule.Matchers) {
	if rules == nil {
		rules = rule.Empty
	}
	e.mu.Lock()
	e.rules = rules
	e.mu.Unlock()
}

// Ingest processes one trigger/recovery event and records its conclusion.
func (e *Engine) Ingest(ctx context.Context, a alert.Alert) Decision {
	if a.Firing() {
		return e.ingestFiring(a)
	}
	return e.ingestResolved(a)
}

func (e *Engine) ingestFiring(a alert.Alert) Decision {
	now := e.clk.Now()
	rules := e.currentRules()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if existed && en.Status == alert.StateResolved {
			// Recovery already on record: this firing is out of order.
			// It resurrects nothing and notifies nothing; payload is left
			// untouched (distinct from duplicate handling below).
			en.LastSeen = now
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeOutOfOrder,
				Reason:  "firing arrives after resolved state; state left unchanged, no notification",
			}
			return
		}

		if existed {
			// Refresh label/timing payload but keep the Notified latch.
			en.Alert = mergeAlert(en.Alert, a)
			en.LastSeen = now
			en.UpdatedAt = now
		} else {
			en.Alert = a
			en.FirstSeen = now
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StatePending
		}

		// Repeat firing of an active, already-notified condition.
		if en.Notified {
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeDuplicate,
				Reason:  "pending fingerprint already notified; repeat firing deduped",
			}
			return
		}

		// Pending but never notified (earlier events were muted). Evaluate
		// current rules: still muted -> muted duplicate; cleared -> first
		// notification happens now.
		if sid := rules.MatchSilence(en.Alert.Labels, now); sid != "" {
			en.Silenced, en.SilenceBy = true, sid
			en.Suppressed, en.SuppressBy = false, ""
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeSilenced,
				RuleID:  sid,
				Reason:  "firing matches active silence " + sid + "; tracked, no notification",
			}
			return
		}
		if iid := e.matchInhibit(rules, en, active); iid != "" {
			en.Suppressed, en.SuppressBy = true, iid
			en.Silenced, en.SilenceBy = false, ""
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeSuppressed,
				RuleID:  iid,
				Reason:  "firing inhibited by active source via rule " + iid + "; tracked, no notification",
			}
			return
		}

		decision = e.fireNotification(en, "firing passes silence then inhibition; dispatch")
	})

	e.finalize(decision)
	return decision
}

func (e *Engine) ingestResolved(a alert.Alert) Decision {
	now := e.clk.Now()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if !existed {
			// First sighting is a recovery: orphan. Track it in resolved
			// state so repeated recoveries become explicit duplicates and
			// later firings are explicit out-of-order events.
			en.Alert = a
			en.FirstSeen = now
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StateResolved
			decision = Decision{
				Alert:   a,
				Outcome: alert.OutcomeOrphanResolve,
				Reason:  "recovery with no prior firing; tracked as resolved, no notification",
			}
			return
		}

		if en.Status == alert.StateResolved {
			en.LastSeen = now
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeDuplicate,
				Reason:  "recovery for already-resolved fingerprint; duplicate recovery",
			}
			return
		}

		// Pending -> resolved.
		en.Alert = mergeAlert(en.Alert, a)
		en.LastSeen = now
		en.UpdatedAt = now
		en.Status = alert.StateResolved

		if !en.Notified {
			// Muted its entire active life; nothing fired, nothing recovers.
			ruleID := en.SilenceBy
			if ruleID == "" {
				ruleID = en.SuppressBy
			}
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeResolvedNoNotification,
				RuleID:  ruleID,
				Reason:  "resolved while muted for whole active life; no prior firing notification",
			}
			return
		}
		decision = e.resolveNotification(en, "recovery matches prior firing; resolution notification dispatched")
	})

	e.finalize(decision)
	return decision
}

// matchInhibit finds an inhibiting rule for en, excluding en itself from
// candidate sources so an alert can never inhibit itself.
func (e *Engine) matchInhibit(rules *rule.Matchers, en *state.Entry, active state.ActiveLabelsFunc) string {
	all := active()
	sources := make([]map[string]string, 0, len(all))
	for fp, labels := range all {
		if fp == en.Alert.Fingerprint {
			continue
		}
		sources = append(sources, labels)
	}
	return rules.MatchInhibit(en.Alert.Labels, sources)
}

// fireNotification latches notification and builds a firing decision.
// Called only within a store.Apply callback.
func (e *Engine) fireNotification(en *state.Entry, reason string) Decision {
	en.Notified = true
	en.Suppressed, en.SuppressBy = false, ""
	en.Silenced, en.SilenceBy = false, ""
	n := e.makeNotification(en.Alert, alert.KindFiring)
	return Decision{
		Alert:    en.Alert,
		Outcome:  alert.OutcomeNotified,
		Reason:   reason,
		Messages: []notify.Notification{n},
	}
}

func (e *Engine) resolveNotification(en *state.Entry, reason string) Decision {
	n := e.makeNotification(en.Alert, alert.KindResolved)
	return Decision{
		Alert:    en.Alert,
		Outcome:  alert.OutcomeResolvedNotification,
		Reason:   reason,
		Messages: []notify.Notification{n},
	}
}

func (e *Engine) makeNotification(a alert.Alert, kind alert.EventKind) notify.Notification {
	seq := atomic.AddUint64(&e.notifSeq, 1)
	labels := make(map[string]string, len(a.Labels))
	for k, v := range a.Labels {
		labels[k] = v
	}
	return notify.Notification{
		ID:          a.Fingerprint + "-" + kind.String() + "-" + strconv.FormatUint(seq, 10),
		Fingerprint: a.Fingerprint,
		Group:       alert.GroupKey(labels, e.groupBy),
		Kind:        kind,
		Labels:      labels,
		At:          e.clk.Now(),
	}
}

// finalize records the decision and dispatches its messages. Inputs were
// fixed inside the store lock; dispatch itself never touches the store.
func (e *Engine) finalize(d Decision) {
	e.rec.RecordDecision(d)
	for i := range d.Messages {
		n := d.Messages[i]
		fp := n.Fingerprint
		kind := n.Kind
		e.disp.Dispatch(n, func() bool {
			cur, ok := e.store.Get(fp)
			if !ok {
				return false
			}
			switch kind {
			case alert.KindFiring:
				return cur.Status == alert.StatePending
			case alert.KindResolved:
				return cur.Status == alert.StateResolved
			}
			return true
		})
	}
}

// mergeAlert overlays a newly arrived event onto the stored payload.
func mergeAlert(stored alert.Alert, incoming alert.Alert) alert.Alert {
	out := stored
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	for k, v := range incoming.Labels {
		out.Labels[k] = v
	}
	if !incoming.StartsAt.IsZero() && (out.StartsAt.IsZero() || incoming.StartsAt.Before(out.StartsAt)) {
		out.StartsAt = incoming.StartsAt
	}
	if !incoming.EndsAt.IsZero() && (out.EndsAt.IsZero() || incoming.EndsAt.After(out.EndsAt)) {
		out.EndsAt = incoming.EndsAt
	}
	out.Kind = incoming.Kind
	return out
}

// WaitIdle blocks until deliveries and retry backoffs have settled.
func (e *Engine) WaitIdle(ctx context.Context) error { return e.disp.Idle(ctx) }

// State returns a value copy of the tracked state for inspection.
func (e *Engine) State() map[string]state.Entry { return e.store.Snapshot() }

// Close stops background delivery.
func (e *Engine) Close() { e.disp.Stop() }
