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
// In addition, events for one fingerprint are serialized by a per-key
// mutex that also covers dispatch: for a given fingerprint the firing of
// round N always reaches every channel before round N's recovery, and a
// follow-up round cannot start delivering until the previous round closed.
// Different fingerprints (and different groups/channels) still proceed in
// parallel. Dispatch inputs are fixed under the store lock; Reload only
// swaps an atomic rule pointer and an event evaluates against the snapshot
// it captured on entry.
type Engine struct {
	clk      clock.Clock
	store    *state.Store
	disp     *notify.Dispatcher
	rec      *Recorder
	groupBy  []string
	notifSeq uint64

	mu    sync.Mutex
	rules *rule.Matchers

	// fpLocks has one independent mutex per fingerprint; it only ever wraps
	// state transition + dispatch and never the other way around, so a slow
	// group on one key cannot block unrelated keys.
	fpLocks sync.Map // string -> *sync.Mutex
}

// lockFingerprint serializes the whole handling (state transition plus
// all-channel dispatch) of events belonging to one fingerprint.
func (e *Engine) lockFingerprint(fp string) func() {
	v, _ := e.fpLocks.LoadOrStore(fp, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
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
//
// Events for one fingerprint are handled strictly one at a time, including
// their channel deliveries, so multi-round sequences are delivered in the
// exact order the state machine evolves them.
func (e *Engine) Ingest(ctx context.Context, a alert.Alert) Decision {
	unlock := e.lockFingerprint(a.Fingerprint)
	defer unlock()

	var decision Decision
	if a.Firing() {
		decision = e.ingestFiring(a)
	} else {
		decision = e.ingestResolved(a)
	}
	e.finalize(decision)
	return decision
}

func (e *Engine) ingestFiring(a alert.Alert) Decision {
	now := e.clk.Now()
	rules := e.currentRules()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if existed && en.Status == alert.StateResolved {
			if en.OrphanResolved {
				// Recovery arrived before any firing: an orphan already
				// owns the resolved state. This firing is out of order; it
				// opens no round and notifies nothing (payload untouched).
				en.LastSeen = now
				decision = Decision{
					Alert:   en.Alert,
					Outcome: alert.OutcomeOutOfOrder,
					Reason:  "firing arrives after an orphan recovery; state left unchanged, no new round, no notification",
				}
				return
			}
			// Previous round completed (firing -> recovery). A new firing
			// opens a fresh round evaluated on its own merits.
			rn := en.OpenRound(a, now)
			if en.FirstSeen.IsZero() {
				en.FirstSeen = now
			}
			decision = e.evaluateFiring(en, rn, rules, now, active,
				"new firing after previous round resolved; round "+strconv.Itoa(rn.Number)+" opened")
			return
		}

		var rn *state.Round
		if existed {
			// Repeat firing inside the open round: refresh payload/timing
			// but keep this round's notification latch.
			rn = en.CurrentRound()
			en.Alert = mergeAlert(en.Alert, a)
			rn.Alert = en.Alert
			en.LastSeen = now
			en.UpdatedAt = now
		} else {
			// Brand new fingerprint: round 1. OpenRound sets the flat
			// payload/timing; FirstSeen is entry-level for life.
			rn = en.OpenRound(a, now)
			en.FirstSeen = now
		}

		// Repeat firing of an active, already-notified round.
		if rn.FiringNotified {
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeDuplicate,
				Reason:  "round " + strconv.Itoa(rn.Number) + " pending and already notified; repeat firing deduped within the round",
				Round:   rn.Number,
			}
			return
		}

		decision = e.evaluateFiring(en, rn, rules, now, active, "round "+strconv.Itoa(rn.Number)+" firing passes silence then inhibition; dispatch")
	})

	return decision
}

// evaluateFiring applies silence then inhibition to an open round whose
// firing latch is still clear, or dispatches its one firing notification.
// Called only within a store.Apply callback.
func (e *Engine) evaluateFiring(en *state.Entry, rn *state.Round, rules *rule.Matchers, now time.Time, active state.ActiveLabelsFunc, notifyReason string) Decision {
	if sid := rules.MatchSilence(en.Alert.Labels, now); sid != "" {
		rn.Silenced, rn.SilenceBy = true, sid
		rn.Suppressed, rn.SuppressBy = false, ""
		en.SyncFlat()
		return Decision{
			Alert:   en.Alert,
			Outcome: alert.OutcomeSilenced,
			RuleID:  sid,
			Reason:  "round " + strconv.Itoa(rn.Number) + " firing matches active silence " + sid + "; tracked, no notification",
			Round:   rn.Number,
		}
	}
	if iid := e.matchInhibit(rules, en, active); iid != "" {
		rn.Suppressed, rn.SuppressBy = true, iid
		rn.Silenced, rn.SilenceBy = false, ""
		en.SyncFlat()
		return Decision{
			Alert:   en.Alert,
			Outcome: alert.OutcomeSuppressed,
			RuleID:  iid,
			Reason:  "round " + strconv.Itoa(rn.Number) + " firing inhibited by active source via rule " + iid + "; tracked, no notification",
			Round:   rn.Number,
		}
	}
	return e.fireNotification(en, rn, notifyReason)
}

func (e *Engine) ingestResolved(a alert.Alert) Decision {
	now := e.clk.Now()

	var decision Decision
	e.store.Apply(a.Fingerprint, true, func(en *state.Entry, existed bool, active state.ActiveLabelsFunc) {
		if !existed {
			// First sighting is a recovery with no prior firing: orphan.
			// Track it in resolved state (with no round) so repeated
			// recoveries become explicit duplicates and a later firing is an
			// explicit out-of-order event rather than a new round.
			a.Labels = cloneLabels(a.Labels)
			en.Alert = a
			en.FirstSeen = now
			en.LastSeen = now
			en.UpdatedAt = now
			en.Status = alert.StateResolved
			en.OrphanResolved = true
			decision = Decision{
				Alert:   a,
				Outcome: alert.OutcomeOrphanResolve,
				Reason:  "recovery with no prior firing; tracked as resolved (no round), no notification",
			}
			return
		}

		if en.Status == alert.StateResolved {
			// Repeated recovery: identical conclusion whether the resolved
			// state came from an orphan or from a closed round.
			en.LastSeen = now
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeDuplicate,
				Reason:  "recovery for already-resolved fingerprint (round " + strconv.Itoa(en.RoundNo) + " closed); duplicate recovery",
				Round:   en.RoundNo,
			}
			return
		}

		// Close the open round.
		rn := en.CurrentRound()
		en.Alert = mergeAlert(en.Alert, a)
		rn.Alert = en.Alert
		en.LastSeen = now
		en.UpdatedAt = now
		en.Status = alert.StateResolved
		rn.ResolvedAt = now

		if !rn.FiringNotified {
			// Muted for the whole active life; nothing fired, nothing
			// recovers. The latch belongs to this round, so a later new
			// round still gets its own fresh notifications.
			ruleID := rn.SilenceBy
			if ruleID == "" {
				ruleID = rn.SuppressBy
			}
			decision = Decision{
				Alert:   en.Alert,
				Outcome: alert.OutcomeResolvedNoNotification,
				RuleID:  ruleID,
				Reason:  "round " + strconv.Itoa(rn.Number) + " resolved while muted for whole active life; no prior firing notification",
				Round:   rn.Number,
			}
			return
		}
		rn.ResolveNotified = true
		decision = e.resolveNotification(en, rn, "recovery closes round "+strconv.Itoa(rn.Number)+"; resolution notification dispatched")
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

// fireNotification latches this round's firing notification and builds the
// decision. Called only within a store.Apply callback.
func (e *Engine) fireNotification(en *state.Entry, rn *state.Round, reason string) Decision {
	rn.FiringNotified = true
	rn.Suppressed, rn.SuppressBy = false, ""
	rn.Silenced, rn.SilenceBy = false, ""
	en.SyncFlat()
	n := e.makeNotification(en.Alert, alert.KindFiring, rn.Number)
	return Decision{
		Alert:    en.Alert,
		Outcome:  alert.OutcomeNotified,
		Reason:   reason,
		Messages: []notify.Notification{n},
		Round:    rn.Number,
	}
}

func (e *Engine) resolveNotification(en *state.Entry, rn *state.Round, reason string) Decision {
	n := e.makeNotification(en.Alert, alert.KindResolved, rn.Number)
	return Decision{
		Alert:    en.Alert,
		Outcome:  alert.OutcomeResolvedNotification,
		Reason:   reason,
		Messages: []notify.Notification{n},
		Round:    rn.Number,
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
		Labels:      labels,
		At:          e.clk.Now(),
		Round:       round,
	}
}

// finalize records the decision and dispatches its messages. Inputs were
// fixed inside the store lock; dispatch itself never touches the store.
// Revalidation is round-aware: a firing notification of round r is valid
// only while round r is still open, and a resolution notification once
// round r is closed — a later round does not invalidate either.
func (e *Engine) finalize(d Decision) {
	e.rec.RecordDecision(d)
	for i := range d.Messages {
		n := d.Messages[i]
		fp := n.Fingerprint
		kind := n.Kind
		round := n.Round
		e.disp.Dispatch(n, func() bool {
			cur, ok := e.store.Get(fp)
			if !ok || round <= 0 || round > len(cur.Rounds) {
				return false
			}
			r := cur.Rounds[round-1]
			switch kind {
			case alert.KindFiring:
				return r.Open() && !r.ResolveNotified
			case alert.KindResolved:
				return !r.Open()
			}
			return true
		})
	}
}

// mergeAlert overlays a newly arrived event onto the stored payload. The
// stored label map is never mutated in place (it may be shared with the
// caller or being read concurrently by other goroutines): a fresh merged
// map is allocated every time.
func mergeAlert(stored alert.Alert, incoming alert.Alert) alert.Alert {
	out := stored
	out.Labels = mergeLabels(stored.Labels, incoming.Labels)
	if !incoming.StartsAt.IsZero() && (out.StartsAt.IsZero() || incoming.StartsAt.Before(out.StartsAt)) {
		out.StartsAt = incoming.StartsAt
	}
	if !incoming.EndsAt.IsZero() && (out.EndsAt.IsZero() || incoming.EndsAt.After(out.EndsAt)) {
		out.EndsAt = incoming.EndsAt
	}
	out.Kind = incoming.Kind
	return out
}

// cloneLabels returns a detached copy of labels (nil stays nil).
func cloneLabels(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// mergeLabels overlays incoming pairs onto stored ones into a fresh map.
func mergeLabels(stored, incoming map[string]string) map[string]string {
	merged := make(map[string]string, len(stored)+len(incoming))
	for k, v := range stored {
		merged[k] = v
	}
	for k, v := range incoming {
		merged[k] = v
	}
	return merged
}

// WaitIdle blocks until deliveries and retry backoffs have settled.
func (e *Engine) WaitIdle(ctx context.Context) error { return e.disp.Idle(ctx) }

// State returns a value copy of the tracked state for inspection.
func (e *Engine) State() map[string]state.Entry { return e.store.Snapshot() }

// Close stops background delivery.
func (e *Engine) Close() { e.disp.Stop() }
