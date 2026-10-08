// Package engine wires ingestion, state tracking, silence/inhibition
// rules and local notification dispatch into one routing engine.
package engine

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
// In addition, events for one fingerprint are serialized by a dedicated
// mutex for the whole "decide then dispatch" span: with multi-round faults
// this guarantees that, per channel, round N's resolution is always
// delivered before round N+1's firing even when producers race. Different
// fingerprints still process in parallel (a slow channel blocks only the
// fingerprint currently delivering). Dispatch inputs (the notification
// value) are fixed under the store lock; delivery itself never mutates the
// store. Reload only swaps an atomic rule pointer; an event evaluates
// against the snapshot it captured on entry.
type Engine struct {
	clk      clock.Clock
	store    *state.Store
	disp     *notify.Dispatcher
	rec      *Recorder
	groupBy  []string
	notifSeq uint64

	// fpMu holds one *sync.Mutex per fingerprint, serializing the
	// decide-and-deliver span per fingerprint. Independent fingerprints
	// use independent mutexes and never block one another.
	fpMu sync.Map // string -> *sync.Mutex

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

// lockFingerprint serializes the whole decide-and-deliver span for one
// fingerprint. Only same-fingerprint events contend; the lock is never
// taken while holding the store lock, so distinct fingerprints (distinct
// faults, distinct groups) keep flowing in parallel.
func (e *Engine) lockFingerprint(fp string) func() {
	v, _ := e.fpMu.LoadOrStore(fp, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
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
//
// Events for one fingerprint take that fingerprint's lock for the whole
// decide-and-deliver span, so racing producers can never deliver round
// N+1's firing before round N's resolution. The lock is per fingerprint:
// different faults and different groups stay parallel end to end.
func (e *Engine) Ingest(ctx context.Context, a alert.Alert) Decision {
	unlock := e.lockFingerprint(a.Fingerprint)
	defer unlock()

	var d Decision
	if a.Firing() {
		d = e.ingestFiring(a)
	} else {
		d = e.ingestResolved(a)
	}
	e.finalize(d)
	return d
}

func (e *Engine) ingestFiring(a alert.Alert) Decision {
	now := e.clk.Now()
	rules := e.currentRules()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if existed && en.Status == alert.StateResolved {
			en.LastSeen = now
			if en.Orphan {
				// The resolved state was born from an orphan recovery: a
				// firing can never legitimately open a round for it. This
				// stays out_of_order even though non-orphan resolved states
				// allow a new round to start; the two conclusions are kept
				// distinct on purpose.
				decision = Decision{
					Alert:   en.Alert,
					Round:   en.Round,
					Outcome: alert.OutcomeOutOfOrder,
					Reason:  "firing arrives after an orphan recovery; no round opens, state left unchanged, no notification",
				}
				return
			}

			// A completed, real round followed by a new firing: open the
			// next round. Freeze the finished round in History first; it is
			// never mutated again. The new round gets fresh latches and is
			// evaluated against the rules active at ITS arrival.
			en.History = append(en.History, en.CurrentRound())
			en.Round++
			en.Alert = a
			en.OpenedAt = now
			en.ResolvedAt = time.Time{}
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StatePending
			en.Notified = false
			en.Orphan = false
			en.Suppressed, en.SuppressBy = false, ""
			en.Silenced, en.SilenceBy = false, ""
			decision = e.evaluateFiring(en, rules, active, now,
				"new round "+strconv.Itoa(en.Round)+" opens after prior recovery; firing notification dispatched")
			return
		}

		if existed {
			// Same round: refresh label/timing payload but keep its latches.
			en.Alert = mergeAlert(en.Alert, a)
			en.LastSeen = now
			en.UpdatedAt = now
		} else {
			en.Alert = a
			en.FirstSeen = now
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StatePending
			en.Round = 1
			en.OpenedAt = now
		}

		decision = e.evaluateFiring(en, rules, active, now,
			"round "+strconv.Itoa(en.Round)+" firing passes silence then inhibition; dispatch")
	})

	return decision
}

// evaluateFiring applies the within-round policy order to the current
// (always pending) round: notified latch first, then silence, then
// inhibition, then a real firing notification. Called only inside a
// store.Apply callback.
func (e *Engine) evaluateFiring(en *state.Entry, rules *rule.Matchers, active state.ActiveLabelsFunc, now time.Time, notifyReason string) Decision {
	// Repeat firing of an active, already-notified round. The latch is
	// per round: a later round never reaches here against an old latch.
	if en.Notified {
		return Decision{
			Alert:   en.Alert,
			Round:   en.Round,
			Outcome: alert.OutcomeDuplicate,
			Reason:  "pending round " + strconv.Itoa(en.Round) + " already notified; repeat firing deduped within the round",
		}
	}

	// Pending but never notified (earlier events of THIS round were muted).
	// Evaluate the rules captured at this event's arrival: still muted ->
	// muted duplicate; cleared -> the round's first notification happens now.
	if sid := rules.MatchSilence(en.Alert.Labels, now); sid != "" {
		en.Silenced, en.SilenceBy = true, sid
		en.Suppressed, en.SuppressBy = false, ""
		return Decision{
			Alert:   en.Alert,
			Round:   en.Round,
			Outcome: alert.OutcomeSilenced,
			RuleID:  sid,
			Reason:  "round " + strconv.Itoa(en.Round) + " firing matches active silence " + sid + "; tracked, no notification",
		}
	}
	if iid := e.matchInhibit(rules, en, active); iid != "" {
		en.Suppressed, en.SuppressBy = true, iid
		en.Silenced, en.SilenceBy = false, ""
		return Decision{
			Alert:   en.Alert,
			Round:   en.Round,
			Outcome: alert.OutcomeSuppressed,
			RuleID:  iid,
			Reason:  "round " + strconv.Itoa(en.Round) + " firing inhibited by active source via rule " + iid + "; tracked, no notification",
		}
	}

	return e.fireNotification(en, notifyReason)
}

func (e *Engine) ingestResolved(a alert.Alert) Decision {
	now := e.clk.Now()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if !existed {
			// First sighting is a recovery: orphan. Track it as a record-only
			// resolved episode (round 0, Orphan) so repeated recoveries are
			// explicit duplicates and later firings are explicit
			// out-of-order events; neither path opens a real new round.
			en.Alert = a
			en.FirstSeen = now
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StateResolved
			en.Round = 0
			en.OpenedAt = now
			en.ResolvedAt = now
			en.Orphan = true
			decision = Decision{
				Alert:   a,
				Round:   0,
				Outcome: alert.OutcomeOrphanResolve,
				Reason:  "recovery with no prior firing; tracked as record-only resolved, no notification",
			}
			return
		}

		if en.Status == alert.StateResolved {
			en.LastSeen = now
			decision = Decision{
				Alert:   en.Alert,
				Round:   en.Round,
				Outcome: alert.OutcomeDuplicate,
				Reason:  "recovery for already-resolved round " + strconv.Itoa(en.Round) + "; duplicate recovery",
			}
			return
		}

		// Pending -> resolved: close exactly the current round.
		en.Alert = mergeAlert(en.Alert, a)
		en.LastSeen = now
		en.UpdatedAt = now
		en.Status = alert.StateResolved
		en.ResolvedAt = now

		if !en.Notified {
			// Muted its entire active life; nothing fired, nothing recovers.
			ruleID := en.SilenceBy
			if ruleID == "" {
				ruleID = en.SuppressBy
			}
			decision = Decision{
				Alert:   en.Alert,
				Round:   en.Round,
				Outcome: alert.OutcomeResolvedNoNotification,
				RuleID:  ruleID,
				Reason:  "round " + strconv.Itoa(en.Round) + " resolved while muted for whole active life; no prior firing notification",
			}
			return
		}
		decision = e.resolveNotification(en,
			"recovery closes round "+strconv.Itoa(en.Round)+"; resolution notification dispatched")
	})

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

// fireNotification latches the current round's firing and builds its
// decision. Called only within a store.Apply callback.
func (e *Engine) fireNotification(en *state.Entry, reason string) Decision {
	en.Notified = true
	en.Suppressed, en.SuppressBy = false, ""
	en.Silenced, en.SilenceBy = false, ""
	n := e.makeNotification(en.Alert, alert.KindFiring, en.Round)
	return Decision{
		Alert:    en.Alert,
		Round:    en.Round,
		Outcome:  alert.OutcomeNotified,
		Reason:   reason,
		Messages: []notify.Notification{n},
	}
}

func (e *Engine) resolveNotification(en *state.Entry, reason string) Decision {
	n := e.makeNotification(en.Alert, alert.KindResolved, en.Round)
	return Decision{
		Alert:    en.Alert,
		Round:    en.Round,
		Outcome:  alert.OutcomeResolvedNotification,
		Reason:   reason,
		Messages: []notify.Notification{n},
	}
}

func (e *Engine) makeNotification(a alert.Alert, kind alert.EventKind, round int) notify.Notification {
	seq := atomic.AddUint64(&e.notifSeq, 1)
	labels := make(map[string]string, len(a.Labels))
	for k, v := range a.Labels {
		labels[k] = v
	}
	return notify.Notification{
		ID:          a.Fingerprint + "-r" + strconv.Itoa(round) + "-" + kind.String() + "-" + strconv.FormatUint(seq, 10),
		Fingerprint: a.Fingerprint,
		Group:       alert.GroupKey(labels, e.groupBy),
		Kind:        kind,
		Round:       round,
		Labels:      labels,
		At:          e.clk.Now(),
	}
}

// finalize records the decision and dispatches its messages. Inputs were
// fixed inside the store lock; dispatch never touches the store. The
// revalidation predicate is scoped to the notification's own round, so a
// later round's state change can never brand an earlier round's resolution
// stale (or vice versa). At the engine level same-fingerprint ingestion is
// already serialized across the whole dispatch span, which keeps the
// predicate stable while delivery (including retries) runs; it remains
// defense in depth and keeps direct dispatcher use meaningful.
func (e *Engine) finalize(d Decision) {
	e.rec.RecordDecision(d)
	for i := range d.Messages {
		n := d.Messages[i]
		fp := n.Fingerprint
		kind := n.Kind
		round := n.Round
		e.disp.Dispatch(n, func() bool {
			en, ok := e.store.Get(fp)
			if !ok {
				return false
			}
			r, found := en.RoundByNumber(round)
			if !found {
				return false
			}
			if kind == alert.KindFiring {
				return r.Status == alert.StatePending
			}
			return r.Status == alert.StateResolved
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
