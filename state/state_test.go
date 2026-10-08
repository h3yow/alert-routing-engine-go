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

func TestRoundsAccumulateWithoutOverwrite(t *testing.T) {
	s := NewStore()
	at := func(n int) time.Time { return time.Unix(int64(n), 0) }

	_, _ = s.Apply("fp", true, func(e *Entry, existed bool, _ ActiveLabelsFunc) {
		r := e.OpenRound(alert.Alert{Fingerprint: "fp", Labels: map[string]string{"h": "1"}}, at(1))
		r.FiringNotified = true
		e.SyncFlat()
	})
	got, _ := s.Get("fp")
	if got.RoundNo != 1 || !got.Notified || got.Status != alert.StatePending {
		t.Fatalf("round 1 state wrong: %+v", got)
	}

	// Close round 1, then open and close round 2.
	_, _ = s.Apply("fp", true, func(e *Entry, _ bool, _ ActiveLabelsFunc) {
		r := e.CurrentRound()
		r.ResolveNotified = true
		r.ResolvedAt = at(2)
		e.Status = alert.StateResolved
	})
	_, _ = s.Apply("fp", true, func(e *Entry, _ bool, _ ActiveLabelsFunc) {
		r := e.OpenRound(alert.Alert{Fingerprint: "fp", Labels: map[string]string{"h": "2"}}, at(3))
		r.FiringNotified = true
		r.ResolveNotified = true
		r.ResolvedAt = at(4)
		e.Status = alert.StateResolved
	})

	got, _ = s.Get("fp")
	if got.RoundNo != 2 || len(got.Rounds) != 2 {
		t.Fatalf("want 2 rounds, got %+v", got)
	}
	r1, r2 := got.Rounds[0], got.Rounds[1]
	if r1.Number != 1 || !r1.FiringNotified || !r1.ResolveNotified || r1.Open() {
		t.Fatalf("round 1 history overwritten: %+v", r1)
	}
	if r1.Alert.Labels["h"] != "1" || r2.Alert.Labels["h"] != "2" {
		t.Fatalf("per-round payloads not independent: r1=%v r2=%v", r1.Alert.Labels, r2.Alert.Labels)
	}
	if !r1.ResolvedAt.Before(r2.FiredAt) {
		t.Fatal("round 1 must resolve before round 2 fires")
	}
	if got.CurrentRound() == nil {
		t.Fatal("CurrentRound nil after 2 rounds")
	}

	// Clone detaches round label maps.
	mut := got.Clone()
	mut.Rounds[0].Alert.Labels["h"] = "x"
	if again, _ := s.Get("fp"); again.Rounds[0].Alert.Labels["h"] != "1" {
		t.Fatal("clone shared round label map")
	}
}
