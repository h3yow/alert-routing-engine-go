// Package state holds the tracked lifecycle state of alerts, keyed by
// fingerprint. All mutations happen inside Apply under a single write
// lock, so concurrent producers never observe or produce torn updates.
package state

import (
	"sync"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
)

// Round is an immutable view of one fault episode: from the firing that
// opened it to the recovery that closed it. A fingerprint can live through
// many rounds (flapping); each keeps its own notification latches, so an
// earlier round can never be overwritten by or deduplicated against a later
// one.
type Round struct {
	// Number is the 1-based ordinal of the episode for its fingerprint.
	Number int
	Alert  alert.Alert
	Status alert.Status
	// OpenedAt / ResolvedAt bracket the episode. ResolvedAt is zero while
	// the round is still pending.
	OpenedAt   time.Time
	ResolvedAt time.Time

	// FiringNotified / ResolveNotified are the per-round notification
	// latches: exactly one firing and (when applicable) one resolution.
	FiringNotified  bool
	ResolveNotified bool
	// Orphan marks a round opened directly by a recovery with no firing.
	Orphan bool

	// Suppressed / Silenced record the most recent policy conclusion seen
	// by the round, so state stays observable while no notification is sent.
	Suppressed bool
	SuppressBy string
	Silenced   bool
	SilenceBy  string
}

func cloneRound(r Round) Round {
	cp := r
	cp.Alert.Labels = cloneLabels(r.Alert.Labels)
	return cp
}

// Entry is the tracked state for one alert fingerprint.
//
// The top-level fields always describe the LATEST round (pending or just
// resolved). Completed-and-superseded rounds are frozen in History, which
// is appended to exactly once, when a new round opens.
type Entry struct {
	Alert     alert.Alert
	Status    alert.Status
	FirstSeen time.Time
	LastSeen  time.Time
	UpdatedAt time.Time

	// Round is the 1-based ordinal of the latest episode for this entry.
	Round int
	// OpenedAt / ResolvedAt bracket the latest round.
	OpenedAt   time.Time
	ResolvedAt time.Time
	// Orphan marks that the latest (resolved) round was opened by a
	// recovery with no prior firing.
	Orphan bool

	// Notified records that a firing notification was already dispatched
	// for the CURRENT round. It is the per-round dedup latch: a repeat
	// firing can never notify twice within a round, and a new round resets
	// it so the next episode notifies again.
	Notified bool
	// Suppressed / Silenced record the most recent policy conclusion so
	// state remains observable while no notification is sent.
	Suppressed bool
	SuppressBy string
	Silenced   bool
	SilenceBy  string

	// History holds immutable snapshots of rounds superseded by a later
	// round. Entries are never mutated after append.
	History []Round
}

// Clone returns a value copy safe to read without holding the lock.
func (e Entry) Clone() Entry {
	cp := e
	cp.Alert.Labels = cloneLabels(e.Alert.Labels)
	if e.History != nil {
		cp.History = make([]Round, len(e.History))
		for i, r := range e.History {
			cp.History[i] = cloneRound(r)
		}
	}
	return cp
}

// CurrentRound returns an immutable view of the latest round built from
// the top-level fields.
func (e Entry) CurrentRound() Round {
	return Round{
		Number:          e.Round,
		Alert:           e.Alert,
		Status:          e.Status,
		OpenedAt:        e.OpenedAt,
		ResolvedAt:      e.ResolvedAt,
		FiringNotified:  e.Notified,
		ResolveNotified: e.Status == alert.StateResolved && e.Notified,
		Orphan:          e.Orphan,
		Suppressed:      e.Suppressed,
		SuppressBy:      e.SuppressBy,
		Silenced:        e.Silenced,
		SilenceBy:       e.SilenceBy,
	}
}

// AllRounds returns completed rounds followed by the latest round, in
// round order. The returned slice and its label maps are independent copies.
func (e Entry) AllRounds() []Round {
	out := make([]Round, 0, len(e.History)+1)
	for _, r := range e.History {
		out = append(out, cloneRound(r))
	}
	out = append(out, e.CurrentRound())
	return out
}

// RoundByNumber returns the immutable view of round n and true when it
// exists (either frozen in History or the latest round).
func (e Entry) RoundByNumber(n int) (Round, bool) {
	for _, r := range e.History {
		if r.Number == n {
			return cloneRound(r), true
		}
	}
	cur := e.CurrentRound()
	if cur.Number == n {
		return cur, true
	}
	return Round{}, false
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
