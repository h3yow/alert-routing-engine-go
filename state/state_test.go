package state

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
)

func TestApplyCreateAndLifecycle(t *testing.T) {
	s := NewStore()

	if _, ok := s.Get("x"); ok {
		t.Fatal("empty store returned an entry")
	}

	e, existed := s.Apply("x", true, func(e *Entry, existed bool, active ActiveLabelsFunc) {
		if existed {
			t.Fatal("new fingerprint reported as existed")
		}
		e.Status = alert.StatePending
		e.Notified = true
		e.Alert = alert.Alert{Fingerprint: "x", Labels: map[string]string{"host": "n1"}}
	})
	if existed || e.Notified != true {
		t.Fatalf("apply result wrong: existed=%v entry=%+v", existed, e)
	}

	if got := len(s.ActiveFingerprints()); got != 1 {
		t.Fatalf("active fingerprints = %d, want 1", got)
	}
	if labels := s.ActiveLabels(); labels["x"]["host"] != "n1" {
		t.Fatalf("active labels wrong: %v", labels)
	}

	// Second Apply sees existed=true and can transition to resolved.
	_, existed = s.Apply("x", true, func(e *Entry, existed bool, active ActiveLabelsFunc) {
		if !existed {
			t.Fatal("existing fingerprint reported as new")
		}
		e.Status = alert.StateResolved
	})
	if len(s.ActiveFingerprints()) != 0 {
		t.Fatal("resolved entry still listed as active")
	}
}

func TestSnapshotAndGetAreValueCopies(t *testing.T) {
	s := NewStore()
	s.Apply("x", true, func(e *Entry, existed bool, active ActiveLabelsFunc) {
		e.Alert = alert.Alert{Fingerprint: "x", Labels: map[string]string{"host": "n1"}}
	})
	g, _ := s.Get("x")
	g.Alert.Labels["host"] = "mutated"
	if again, _ := s.Get("x"); again.Alert.Labels["host"] != "n1" {
		t.Fatal("Get returned a shared map")
	}
	snap := s.Snapshot()
	snap["x"].Alert.Labels["host"] = "mutated2"
	if again, _ := s.Get("x"); again.Alert.Labels["host"] != "n1" {
		t.Fatal("Snapshot returned a shared map")
	}
}

func TestConcurrentApplyIsSerialized(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Apply("k", true, func(e *Entry, existed bool, active ActiveLabelsFunc) {
				// Read-modify-write that would race without the lock.
				cur := e.Alert.Labels["n"]
				e.Alert = alert.Alert{Fingerprint: "k", Labels: map[string]string{"n": cur + "x"}}
			})
		}()
	}
	wg.Wait()
	e, _ := s.Get("k")
	if got := len(e.Alert.Labels["n"]); got != 100 {
		t.Fatalf("lost updates under concurrency: marker len=%d want 100", got)
	}
}

// openRound models the engine's round bookkeeping on top of the store:
// complete the current round, freeze it into History, then open the next.
func openRound(s *Store, fp string, round int, resolved bool, notified bool, ts time.Time) {
	s.Apply(fp, true, func(e *Entry, existed bool, _ ActiveLabelsFunc) {
		if !existed {
			e.Round = round - 1
		}
		if e.Round == round-1 && e.Status == alert.StateResolved {
			// Freeze the finished round exactly as the engine does.
			e.History = append(e.History, e.CurrentRound())
			e.Round = round
		} else if !existed {
			e.Round = round
		}
		e.Alert = alert.Alert{Fingerprint: fp, Labels: map[string]string{"r": string(rune('a' + round - 1))}}
		e.Notified = notified
		e.Status = alert.StatePending
		e.OpenedAt = ts
		if resolved {
			e.Status = alert.StateResolved
			e.ResolvedAt = ts.Add(time.Second)
		}
	})
}

func TestRoundHistoryPreservedAcrossRounds(t *testing.T) {
	s := NewStore()
	openRound(s, "f", 1, true, true, time.Unix(10, 0))
	openRound(s, "f", 2, true, true, time.Unix(20, 0))
	openRound(s, "f", 3, false, true, time.Unix(30, 0))

	e, ok := s.Get("f")
	if !ok {
		t.Fatal("missing entry")
	}
	if e.Round != 3 || e.Status != alert.StatePending {
		t.Fatalf("latest round = %d/%s, want 3/pending", e.Round, e.Status)
	}
	rounds := e.AllRounds()
	if len(rounds) != 3 {
		t.Fatalf("AllRounds len = %d, want 3", len(rounds))
	}
	for i, r := range rounds {
		if r.Number != i+1 {
			t.Fatalf("round[%d].Number = %d", i, r.Number)
		}
	}
	// Each completed round keeps its own firing/resolve latches and payload.
	r1 := rounds[0]
	if !r1.FiringNotified || !r1.ResolveNotified || r1.Status != alert.StateResolved {
		t.Fatalf("round 1 view wrong: %+v", r1)
	}
	if r1.Alert.Labels["r"] != "a" {
		t.Fatalf("round 1 payload overwritten: %v", r1.Alert.Labels)
	}
	r2 := rounds[1]
	if r2.Alert.Labels["r"] != "b" || !r2.FiringNotified {
		t.Fatalf("round 2 view wrong: %+v", r2)
	}
	r3 := rounds[2]
	if r3.ResolveNotified || r3.Status != alert.StatePending || !r3.FiringNotified {
		t.Fatalf("round 3 (pending) view wrong: %+v", r3)
	}
	if got, ok := e.RoundByNumber(2); !ok || got.ResolvedAt.IsZero() {
		t.Fatalf("RoundByNumber(2) wrong: %+v ok=%v", got, ok)
	}
}

func TestRoundHistoryIsolatedInClone(t *testing.T) {
	s := NewStore()
	openRound(s, "f", 1, true, true, time.Unix(10, 0))
	openRound(s, "f", 2, false, false, time.Unix(20, 0))

	cp, _ := s.Get("f")
	// Mutating the snapshot's history labels must not reach the store.
	cp.History[0].Alert.Labels["r"] = "MUTATED"
	cp.Alert.Labels["r"] = "MUTATED2"
	again, _ := s.Get("f")
	if again.History[0].Alert.Labels["r"] != "a" {
		t.Fatal("history label map shared between Clone and store")
	}
	if again.Alert.Labels["r"] != "b" {
		t.Fatal("current round label map shared between Clone and store")
	}
}
