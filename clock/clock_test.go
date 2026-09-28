package clock

import (
	"testing"
	"time"
)

func TestFakeClockTimerFiresOnlyOnAdvance(t *testing.T) {
	t0 := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	c := NewFakeClock(t0)

	ch := c.After(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired without Advance")
	default:
	}

	c.Advance(4 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired one second early")
	default:
	}

	c.Advance(1 * time.Second)
	select {
	case got := <-ch:
		if !got.Equal(t0.Add(5 * time.Second)) {
			t.Fatalf("fired at %v, want %v", got, t0.Add(5*time.Second))
		}
	default:
		t.Fatal("timer did not fire after reaching deadline")
	}
}

func TestFakeClockFiresInDeadlineOrder(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	later := c.After(10 * time.Second)
	earlier := c.After(1 * time.Second)
	c.Advance(10 * time.Second)
	<-earlier
	<-later
}

func TestRealClockSatisfiesInterface(t *testing.T) {
	var _ Clock = NewRealClock()
	var _ Clock = NewFakeClock(time.Now())
}
