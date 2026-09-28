package notify

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/alert-routing-engine-go/clock"
)

// sortChannels orders channels by name for deterministic delivery.
func sortChannels(cs []Channel) {
	sort.Slice(cs, func(i, j int) bool { return cs[i].Name() < cs[j].Name() })
}

// Config controls dispatcher retry/backoff behavior.
type Config struct {
	// MaxAttempts is the total number of send attempts (1 disables retry).
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// DefaultConfig gives bounded retry parameters suitable for local runs.
func DefaultConfig() Config {
	return Config{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}
}

// Dispatcher fans notifications out to local channels. Deliveries are
// serialized per (channel, group), so the same group can never receive
// concurrent (possibly duplicate) deliveries. Retries use exponential
// backoff driven by the injected clock; status transitions live in the
// engine and are never altered by retry mechanics.
type Dispatcher struct {
	mu       sync.Mutex
	clk      clock.Clock
	cfg      Config
	channels map[string]Channel
	stopped  bool
	onResult func(Result)

	// groupMu has one independent mutex per (channel, group); it never
	// takes d.mu, so config swaps cannot deadlock against an in-flight
	// attempt holding a group lock.
	groupMu sync.Map // string -> *sync.Mutex

	results chan Result
	wg      sync.WaitGroup
	stopCh  chan struct{}
}

// NewDispatcher constructs a dispatcher holding a copy of the channel set.
func NewDispatcher(clk clock.Clock, channels []Channel, cfg Config) *Dispatcher {
	if cfg.MaxAttempts < 1 {
		cfg = DefaultConfig()
	}
	d := &Dispatcher{
		clk:      clk,
		cfg:      cfg,
		channels: make(map[string]Channel, len(channels)),
		results:  make(chan Result, 4096),
		stopCh:   make(chan struct{}),
	}
	for _, ch := range channels {
		d.channels[ch.Name()] = ch
	}
	return d
}

// ReplaceChannels atomically swaps the live channel set. Tasks already
// queued against removed channels settle with DropChannelGone; tasks not
// yet started target the new set at execution time.
func (d *Dispatcher) ReplaceChannels(channels []Channel) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	next := make(map[string]Channel, len(channels))
	for _, ch := range channels {
		next[ch.Name()] = ch
	}
	d.channels = next
}

// groupLock returns the mutex serializing one (channel, group) pair.
func (d *Dispatcher) groupLock(channel, group string) *sync.Mutex {
	key := channel + "\x00" + group
	v, _ := d.groupMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Dispatch delivers the notification to every channel currently in the
// set synchronously, in channel-name order. Deliveries still serialize
// per (channel, group) and retry with backoff; doing the work on the
// caller's goroutine makes the delivery conclusion consistent with the
// state observed at decision time (a later ingest can no longer outrun
// an earlier notification and make it look stale). The result callback
// fires before Dispatch returns.
func (d *Dispatcher) Dispatch(n Notification, revalidate func() bool) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	targets := make([]Channel, 0, len(d.channels))
	for _, ch := range d.channels {
		targets = append(targets, ch)
	}
	d.mu.Unlock()
	sortChannels(targets)

	for _, ch := range targets {
		d.run(ch, n, revalidate)
	}
}

// DispatchAsync is Dispatch with one goroutine per channel, for callers
// that do not want backoff to block ingestion. Group serialization still
// applies. Use Idle to wait for settlement.
func (d *Dispatcher) DispatchAsync(n Notification, revalidate func() bool) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	targets := make([]Channel, 0, len(d.channels))
	for _, ch := range d.channels {
		targets = append(targets, ch)
	}
	d.wg.Add(len(targets))
	d.mu.Unlock()
	sortChannels(targets)
	for _, ch := range targets {
		go func(ch Channel) {
			defer d.wg.Done()
			d.run(ch, n, revalidate)
		}(ch)
	}
}

func (d *Dispatcher) backoff(attempt int) time.Duration {
	dur := d.cfg.BaseBackoff << uint(attempt-1)
	if dur > d.cfg.MaxBackoff || dur <= 0 {
		dur = d.cfg.MaxBackoff
	}
	return dur
}

func (d *Dispatcher) emit(r Result) {
	if d.onResult != nil {
		d.onResult(r)
		return
	}
	d.results <- r
}

func (d *Dispatcher) run(ch Channel, n Notification, revalidate func() bool) {
	name := ch.Name()

	// Serialize deliveries for this (channel, group); only one attempt
	// loop at a time can touch a group through this channel. The key uses
	// the channel name, which stays stable across a ReplaceChannels swap.
	d.groupLock(name, n.Group).Lock()
	defer d.groupLock(name, n.Group).Unlock()

	ctx := context.Background()
	var lastErr error
	for attempt := 1; attempt <= d.cfg.MaxAttempts; attempt++ {
		// Re-resolve the channel before every attempt (including the
		// first and each retry) so a reload that removes or replaces the
		// channel is observed immediately and reported as channel_gone,
		// never retried against a stale sink or confused with exhaustion.
		d.mu.Lock()
		live, exists := d.channels[name]
		stopped := d.stopped
		d.mu.Unlock()
		if stopped || !exists {
			d.emit(Result{Notification: n, Channel: name, Attempts: attempt - 1, DropReason: DropChannelGone, Err: lastErr, At: d.clk.Now()})
			return
		}
		ch = live

		if revalidate != nil && !revalidate() {
			d.emit(Result{Notification: n, Channel: name, Attempts: attempt - 1, DropReason: DropStale, Err: lastErr, At: d.clk.Now()})
			return
		}
		err := ch.Send(ctx, n)
		if err == nil {
			d.emit(Result{Notification: n, Channel: name, Delivered: true, Attempts: attempt, At: d.clk.Now()})
			return
		}
		lastErr = err
		if attempt == d.cfg.MaxAttempts {
			break
		}
		// Wait for backoff; stop interrupts the wait.
		select {
		case <-d.clk.After(d.backoff(attempt)):
		case <-d.stopCh:
			d.emit(Result{Notification: n, Channel: name, Attempts: attempt, DropReason: DropChannelGone, Err: lastErr, At: d.clk.Now()})
			return
		}
	}
	d.emit(Result{Notification: n, Channel: name, Attempts: d.cfg.MaxAttempts, DropReason: DropExhausted, Err: lastErr, At: d.clk.Now()})
}

// SetResultSink installs a synchronous callback invoked from inside each
// delivery goroutine at its terminal point. When set, results are not also
// sent to the Results channel. It must be called before the first Dispatch.
func (d *Dispatcher) SetResultSink(fn func(Result)) {
	d.mu.Lock()
	d.onResult = fn
	d.mu.Unlock()
}

// Results returns the buffered stream of terminal delivery results.
func (d *Dispatcher) Results() <-chan Result { return d.results }

// Idle blocks until every enqueued delivery and its retry backoff settles.
func (d *Dispatcher) Idle(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop prevents new dispatches and waits for queued work to settle, then
// closes the results stream.
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	close(d.stopCh)
	d.mu.Unlock()
	d.wg.Wait()
	close(d.results)
}
