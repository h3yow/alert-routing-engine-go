package notify

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/alert-routing-engine-go/clock"
)

// scriptedChannel fails the first failN sends, then succeeds. It also
// tracks peak concurrency overall and within a single group.
type scriptedChannel struct {
	mu       sync.Mutex
	name     string
	failN    int32
	active   int32
	maxAct   int32
	inGroup  map[string]int32
	maxGroup map[string]int32
	sends    int32
}

func newScriptedChannel(name string, failN int32) *scriptedChannel {
	return &scriptedChannel{
		name: name, failN: failN,
		inGroup: map[string]int32{}, maxGroup: map[string]int32{},
	}
}

func (c *scriptedChannel) Name() string { return c.name }

func (c *scriptedChannel) Send(ctx context.Context, n Notification) error {
	c.mu.Lock()
	c.inGroup[n.Group]++
	if c.inGroup[n.Group] > c.maxGroup[n.Group] {
		c.maxGroup[n.Group] = c.inGroup[n.Group]
	}
	c.mu.Unlock()

	active := atomic.AddInt32(&c.active, 1)
	for {
		old := atomic.LoadInt32(&c.maxAct)
		if active <= old || atomic.CompareAndSwapInt32(&c.maxAct, old, active) {
			break
		}
	}
	atomic.AddInt32(&c.sends, 1)
	// Yield repeatedly to maximize the chance that an unsynchronized
	// implementation would show overlapping sends; no real-time waiting.
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}
	atomic.AddInt32(&c.active, -1)

	c.mu.Lock()
	c.inGroup[n.Group]--
	c.mu.Unlock()

	if atomic.AddInt32(&c.failN, -1) >= 0 {
		return fmt.Errorf("simulated failure")
	}
	return nil
}

type failingChannel struct{ name string }

func (c failingChannel) Name() string { return c.name }
func (c failingChannel) Send(ctx context.Context, n Notification) error {
	return fmt.Errorf("always down")
}

func waitResults(t *testing.T, d *Dispatcher, want int) []Result {
	t.Helper()
	out := make([]Result, 0, want)
	deadline := time.After(2 * time.Second)
	for len(out) < want {
		select {
		case r := <-d.Results():
			out = append(out, r)
		case <-deadline:
			t.Fatalf("timed out with %d/%d results", len(out), want)
		}
	}
	return out
}

func notif(group, fp string) Notification {
	return Notification{ID: fp + "-id", Fingerprint: fp, Group: group, At: time.Now()}
}

func TestDispatchRetriesWithBackoffThenSucceeds(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(0, 0))
	ch := newScriptedChannel("mem", 2) // fail twice, succeed 3rd
	d := NewDispatcher(fc, []Channel{ch}, Config{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	settled := make(chan struct{})
	lastResult := make(chan Result, 1)
	go func() {
		d.Dispatch(notif("g1", "a"), nil) // blocks through backoff
		lastResult <- <-d.Results()
		close(settled)
	}()
	pumpUntil(settled, fc, time.Millisecond)

	r := <-lastResult
	if !r.Delivered || r.Attempts != 3 {
		t.Fatalf("want delivered on attempt 3, got %+v", r)
	}
}

func TestDispatchExhaustedIsDistinctFromStale(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(0, 0))

	// Transport failure on every attempt -> retry_exhausted, attempts used.
	d1 := NewDispatcher(fc, []Channel{failingChannel{"dead"}},
		Config{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	settled1 := make(chan struct{})
	got1 := make(chan Result, 1)
	go func() {
		d1.Dispatch(notif("g1", "a"), nil)
		got1 <- <-d1.Results()
		close(settled1)
	}()
	pumpUntil(settled1, fc, time.Millisecond)
	r1 := <-got1
	if r1.Delivered || r1.DropReason != DropExhausted || r1.Attempts != 2 {
		t.Fatalf("exhausted case wrong: %+v", r1)
	}

	// Revalidation false before the first attempt -> stale, zero attempts.
	d2 := NewDispatcher(fc, []Channel{failingChannel{"dead2"}},
		Config{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	d2.Dispatch(notif("g1", "b"), func() bool { return false })
	r2 := waitResults(t, d2, 1)[0]
	if r2.DropReason != DropStale || r2.Attempts != 0 {
		t.Fatalf("stale case wrong: %+v", r2)
	}
}

func TestGroupDeliverySerializedAcrossNotifications(t *testing.T) {
	ch := newScriptedChannel("mem", 0)
	d := NewDispatcher(clock.NewRealClock(), []Channel{ch}, DefaultConfig())
	for i := 0; i < 20; i++ {
		d.DispatchAsync(notif("same-group", fmt.Sprintf("fp-%d", i)), nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Idle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&ch.sends); got != 20 {
		t.Fatalf("sends = %d, want 20", got)
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.maxGroup["same-group"] > 1 {
		t.Fatalf("concurrent deliveries within one group: max=%d", ch.maxGroup["same-group"])
	}
}

func TestInFlightTaskHitsRemovedChannelAsChannelGone(t *testing.T) {
	// Block the channel's Send so the task is in flight, remove the
	// channel, then release the send. The loop must still finish; a retry
	// after removal is reported as channel_gone, distinct from stale.
	fc := clock.NewFakeClock(time.Unix(0, 0))
	block := make(chan struct{})
	holdCh := &holdChannel{name: "mem", block: block, started: make(chan struct{}, 1)}
	d := NewDispatcher(fc, []Channel{holdCh}, Config{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond})
	d.DispatchAsync(notif("g1", "a"), nil)
	<-holdCh.started

	// Replace channels while the first attempt is still in flight.
	d.ReplaceChannels(nil)
	close(block) // first Send returns an error; retry sees a gone channel

	settled := make(chan struct{})
	got := make(chan Result, 1)
	go func() {
		got <- waitResults(t, d, 1)[0]
		close(settled)
	}()
	pumpUntil(settled, fc, time.Millisecond)
	r := <-got
	if r.DropReason != DropChannelGone {
		t.Fatalf("want channel_gone, got %v (delivered=%v attempts=%d)", r.DropReason, r.Delivered, r.Attempts)
	}
}

// holdChannel blocks Send until block is closed, then fails.
type holdChannel struct {
	name    string
	block   chan struct{}
	started chan struct{}
	once    sync.Once
}

func (c *holdChannel) Name() string { return c.name }
func (c *holdChannel) Send(ctx context.Context, n Notification) error {
	c.once.Do(func() { c.started <- struct{}{} })
	<-c.block
	return fmt.Errorf("fails after release")
}

// pumpUntil advances fc by step repeatedly until settled fires. Time
// progression is driven solely by the fake clock; runtime.Gosched only
// yields to let delivery goroutines run and never gates the behavior
// under test.
func pumpUntil(settled <-chan struct{}, fc *clock.FakeClock, step time.Duration) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case <-settled:
			return
		default:
		}
		if time.Now().After(deadline) {
			return
		}
		runtime.Gosched()
		fc.Advance(step)
	}
}
