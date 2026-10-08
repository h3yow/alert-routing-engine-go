// Package state holds the tracked lifecycle state of alerts, keyed by
// fingerprint. All mutations happen inside Apply under a single write
// lock, so concurrent producers never observe or produce torn updates.
package state

import (
	"sync"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
)

// Round is one fault episode of one fingerprint: from the firing that
// opened it to the recovery that closed it. A later firing after closure
// opens a NEW Round; completed rounds stay on record forever and are never
// overwritten by later ones.
type Round struct {
	Number int
	Alert  alert.Alert

	FiredAt    time.Time
	ResolvedAt time.Time // zero while the round is open

	// FiringNotified / ResolveNotified are the per-round notification
	// latches: each round gets at most one firing and one resolution
	// notification, independently of every other round.
	FiringNotified  bool
	ResolveNotified bool

	// Suppressed / Silenced record the most recent policy conclusion for
	// this round so state remains observable while no notification is sent.
	Suppressed bool
	SuppressBy string
	Silenced   bool
	SilenceBy  string
}

// Open reports whether the round has not seen its recovery yet.
func (r Round) Open() bool { return r.ResolvedAt.IsZero() }

// Entry is the tracked state for one alert fingerprint.
//
// The flat fields mirror the CURRENT round (or the entry-level payload for
// an orphan recovery) and are kept for direct inspection. Full per-round
// history, including notification latches, lives in Rounds.
type Entry struct {
	Alert     alert.Alert
	Status    alert.Status
	FirstSeen time.Time
	LastSeen  time.Time
	UpdatedAt time.Time

	// Notified records that the current round's firing notification was
	// already dispatched. It is the per-round dedup latch mirror.
	Notified bool
	// Suppressed / Silenced record the most recent policy conclusion so
	// state remains observable while no notification is sent.
	Suppressed bool
	SuppressBy string
	Silenced   bool
	SilenceBy  string

	// OrphanResolved marks an entry whose first sighting was a recovery
	// with no prior firing. A firing afterwards is out of order, NOT a new
	// round, so the multi-round path cannot swallow orphan handling.
	OrphanResolved bool

	// RoundNo is the current round number: the open round while pending,
	// the most recently closed round while resolved. It stays 0 when only
	// an orphan recovery has ever been seen.
	RoundNo int
	// Rounds holds every episode in order, including the open/current one.
	Rounds []Round
}

// CurrentRound returns a pointer to the current round, or nil when no
// round exists (orphan / never fired).
func (e *Entry) CurrentRound() *Round {
	if e.RoundNo <= 0 || e.RoundNo > len(e.Rounds) {
		return nil
	}
	return &e.Rounds[e.RoundNo-1]
}

// OpenRound starts a new fault episode on an entry: round 1 for a brand
// new entry, or a follow-up round after the previous one closed. It resets
// the flat per-round view. FirstSeen is entry-level and left untouched.
// The incoming alert's labels are copied so stored state never aliases a
// map the caller may keep mutating concurrently.
func (e *Entry) OpenRound(a alert.Alert, at time.Time) *Round {
	a.Labels = cloneLabels(a.Labels)
	num := len(e.Rounds) + 1
	e.Rounds = append(e.Rounds, Round{Number: num, Alert: a, FiredAt: at})
	e.RoundNo = num
	e.Status = alert.StatePending
	e.OrphanResolved = false
	e.Alert = a
	e.LastSeen = at
	e.UpdatedAt = at
	e.Notified = false
	e.Suppressed, e.SuppressBy = false, ""
	e.Silenced, e.SilenceBy = false, ""
	return &e.Rounds[num-1]
}

// SyncFlat mirrors the current round onto the flat inspection fields.
func (e *Entry) SyncFlat() {
	r := e.CurrentRound()
	if r == nil {
		e.Notified = false
		e.Suppressed, e.SuppressBy = false, ""
		e.Silenced, e.SilenceBy = false, ""
		return
	}
	e.Notified = r.FiringNotified
	e.Suppressed, e.SuppressBy = r.Suppressed, r.SuppressBy
	e.Silenced, e.SilenceBy = r.Silenced, r.SilenceBy
}

// Clone returns a value copy safe to read without holding the lock.
func (e Entry) Clone() Entry {
	cp := e
	cp.Alert.Labels = cloneLabels(e.Alert.Labels)
	if e.Rounds != nil {
		cp.Rounds = make([]Round, len(e.Rounds))
		for i := range e.Rounds {
			cp.Rounds[i] = e.Rounds[i]
			cp.Rounds[i].Alert.Labels = cloneLabels(e.Rounds[i].Alert.Labels)
		}
	}
	return cp
}

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

// Store is a fingerprint -> Entry table, safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// NewStore constructs an empty store.
func NewStore() *Store {
	return &Store{entries: make(map[string]*Entry)}
}

// Get returns a value copy of the tracked entry. The second result is
// false when the fingerprint is unknown.
func (s *Store) Get(fp string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[fp]
	if !ok {
		return Entry{}, false
	}
	return e.Clone(), true
}

// Snapshot returns a point-in-time value copy of every entry.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Entry, len(s.entries))
	for fp, e := range s.entries {
		out[fp] = e.Clone()
	}
	return out
}

// Apply runs fn under the write lock on the entry for fp, creating a
// pending entry first when create is true and it is absent. fn observes
// whether the entry previously existed and receives activeLabelsLocked,
// which returns the label sets of currently pending alerts WITHOUT
// re-taking the lock (so it is safe to call inside the callback). The
// resulting value copy is returned for lock-free reads.
func (s *Store) Apply(fp string, create bool, fn func(e *Entry, existed bool, active ActiveLabelsFunc)) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, existed := s.entries[fp]
	if !existed {
		if !create {
			fn(nil, false, s.activeLabelsLocked)
			return Entry{}, false
		}
		e = &Entry{Status: alert.StatePending}
		s.entries[fp] = e
	}
	fn(e, existed, s.activeLabelsLocked)
	return e.Clone(), existed
}

// ActiveLabelsFunc returns pending fingerprints mapped to label copies
// while the store write lock is held by the caller.
type ActiveLabelsFunc func() map[string]map[string]string

func (s *Store) activeLabelsLocked() map[string]map[string]string {
	out := make(map[string]map[string]string, len(s.entries))
	for fp, e := range s.entries {
		if e.Status == alert.StatePending {
			out[fp] = cloneLabels(e.Alert.Labels)
		}
	}
	return out
}

// ActiveFingerprints lists fingerprints currently in pending status.
func (s *Store) ActiveFingerprints() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for fp, e := range s.entries {
		if e.Status == alert.StatePending {
			out = append(out, fp)
		}
	}
	return out
}

// ActiveLabels returns value copies of labels of pending alerts, paired
// with their fingerprints. Used for inhibition source lookups.
func (s *Store) ActiveLabels() map[string]map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[string]string, len(s.entries))
	for fp, e := range s.entries {
		if e.Status == alert.StatePending {
			out[fp] = cloneLabels(e.Alert.Labels)
		}
	}
	return out
}
