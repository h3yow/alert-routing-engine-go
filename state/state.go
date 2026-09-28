// Package state holds the tracked lifecycle state of alerts, keyed by
// fingerprint. All mutations happen inside Apply under a single write
// lock, so concurrent producers never observe or produce torn updates.
package state

import (
	"sync"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
)

// Entry is the tracked state for one alert fingerprint.
type Entry struct {
	Alert     alert.Alert
	Status    alert.Status
	FirstSeen time.Time
	LastSeen  time.Time
	UpdatedAt time.Time

	// Notified records that a firing notification was already dispatched.
	// It is the dedup latch: a repeat firing can never notify twice.
	Notified bool
	// Suppressed / Silenced record the most recent policy conclusion so
	// state remains observable while no notification is sent.
	Suppressed bool
	SuppressBy string
	Silenced   bool
	SilenceBy  string
}

// Clone returns a value copy safe to read without holding the lock.
func (e Entry) Clone() Entry {
	cp := e
	cp.Alert.Labels = cloneLabels(e.Alert.Labels)
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
