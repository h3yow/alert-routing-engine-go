package state

import (
	"sync"
	"testing"

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
