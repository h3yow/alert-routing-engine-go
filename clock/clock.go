// Package clock provides injectable time so tests are deterministic and
// never rely on real sleeps.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts wall-clock reads and one-shot timers.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives at least d after the call.
	After(d time.Duration) <-chan time.Time
}

// RealClock uses the host wall clock and real timers.
type RealClock struct{}

// NewRealClock constructs a real clock.
func NewRealClock() *RealClock { return &RealClock{} }

// Now returns the current wall-clock time.
func (c *RealClock) Now() time.Time { return time.Now() }

// After returns a real timer channel.
func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.NewTimer(d).C
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock is a manually driven clock. Timers only fire when Advance is
// called, which makes retry/backoff behavior fully deterministic.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock constructs a fake clock starting at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the fake current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After registers a timer due at now+d. The channel is buffered so firing
// during Advance never blocks, even if nobody is selecting yet.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		deadline: c.now.Add(d),
		ch:       make(chan time.Time, 1),
	}
	c.timers = append(c.timers, t)
	return t.ch
}

// Advance moves time forward by d and synchronously fires every timer
// whose deadline has been reached, in deadline order. Timers fire once.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.fired && !t.deadline.After(c.now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		t.fired = true
		select {
		case t.ch <- t.deadline:
		default:
		}
	}
	c.mu.Unlock()
}
