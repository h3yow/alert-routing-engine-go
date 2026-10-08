package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
	"github.com/h3yow/alert-routing-engine-go/clock"
	"github.com/h3yow/alert-routing-engine-go/notify"
	"github.com/h3yow/alert-routing-engine-go/rule"
)

var t0 = time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC)

// logDecision prints each input event and the engine's judgment basis.
func logDecision(t *testing.T, a alert.Alert, d Decision) {
	t.Helper()
	t.Logf("input kind=%s fp=%s labels=%v => round=%d outcome=%s rule=%q reason=%q messages=%d",
		a.Kind, a.Fingerprint, a.Labels, d.Round, d.Outcome, d.RuleID, d.Reason, len(d.Messages))
}

type testChannel struct {
	mu       sync.Mutex
	name     string
	failLeft int32
	sent     []notify.Notification
}

func (c *testChannel) Name() string { return c.name }
func (c *testChannel) Send(ctx context.Context, n notify.Notification) error {
	if c.failLeft > 0 {
		c.failLeft--
		return fmt.Errorf("channel %s simulated failure", c.name)
	}
	c.mu.Lock()
	c.sent = append(c.sent, n)
	c.mu.Unlock()
	return nil
}

func (c *testChannel) sentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sent)
}

func firing(fp string, labels map[string]string) alert.Alert {
	return alert.Alert{Fingerprint: fp, Labels: labels, Kind: alert.KindFiring, StartsAt: t0}
}
func resolved(fp string, labels map[string]string) alert.Alert {
	return alert.Alert{Fingerprint: fp, Labels: labels, Kind: alert.KindResolved, EndsAt: t0}
}

func newTestEngine(t *testing.T, clk *clock.FakeClock, chs []notify.Channel, rules *rule.Matchers, dcfg notify.Config) (*Engine, *Recorder) {
	t.Helper()
	rec := NewRecorder()
	e := NewWithDispatchConfig(clk, chs, rules, Config{GroupBy: []string{"alertname", "host"}}, dcfg, rec)
	t.Cleanup(e.Close)
	return e, rec
}

func settle(t *testing.T, e *Engine, fc *clock.FakeClock) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = e.WaitIdle(ctx); close(done) }()
	for {
		select {
		case <-done:
			return
		default:
		}
		fc.Advance(time.Millisecond)
	}
}

func outcomes(rec *Recorder) []alert.Outcome {
	ds := rec.Decisions()
	out := make([]alert.Outcome, len(ds))
	for i, d := range ds {
		out[i] = d.Outcome
	}
	return out
}

func TestDuplicateFiringNotifiesOnce(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := &testChannel{name: "mem"}
	e, rec := newTestEngine(t, fc, []notify.Channel{ch}, nil, notify.DefaultConfig())
	ctx := context.Background()

	ls := map[string]string{"alertname": "CPU", "host": "n1"}
	for i := 0; i < 5; i++ {
		d := e.Ingest(ctx, firing("a", ls))
		logDecision(t, firing("a", ls), d)
	}
	settle(t, e, fc)

	want := []alert.Outcome{alert.OutcomeNotified, alert.OutcomeDuplicate, alert.OutcomeDuplicate, alert.OutcomeDuplicate, alert.OutcomeDuplicate}
	if got := outcomes(rec); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	if n := ch.sentCount(); n != 1 {
		t.Fatalf("channel received %d notifications for 5 identical firings", n)
	}
}

func TestOrphanResolveThenDuplicateThenOutOfOrderFiring(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	e, rec := newTestEngine(t, fc, []notify.Channel{&testChannel{name: "mem"}}, nil, notify.DefaultConfig())
	ctx := context.Background()
	ls := map[string]string{"alertname": "Ghost", "host": "n9"}

	// 1) recovery with no firing -> orphan, distinct conclusion
	d := e.Ingest(ctx, resolved("g", ls))
	logDecision(t, resolved("g", ls), d)
	// 2) repeated recovery -> duplicate, must NOT be swallowed as another orphan
	d = e.Ingest(ctx, resolved("g", ls))
	logDecision(t, resolved("g", ls), d)
	// 3) firing after resolved -> out of order, distinct from orphan/duplicate
	d = e.Ingest(ctx, firing("g", ls))
	logDecision(t, firing("g", ls), d)

	got := outcomes(rec)
	want := []alert.Outcome{alert.OutcomeOrphanResolve, alert.OutcomeDuplicate, alert.OutcomeOutOfOrder}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	entry := e.State()["g"]
	if entry.Status != alert.StateResolved {
		t.Fatalf("state after out-of-order firing = %s, want resolved", entry.Status)
	}
}

func TestNormalResolveSendsResolutionNotification(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := &testChannel{name: "mem"}
	e, rec := newTestEngine(t, fc, []notify.Channel{ch}, nil, notify.DefaultConfig())
	ctx := context.Background()
	ls := map[string]string{"alertname": "CPU", "host": "n1"}

	d := e.Ingest(ctx, firing("a", ls))
	logDecision(t, firing("a", ls), d)
	d = e.Ingest(ctx, resolved("a", ls))
	logDecision(t, resolved("a", ls), d)
	settle(t, e, fc)

	got := outcomes(rec)
	if got[0] != alert.OutcomeNotified || got[1] != alert.OutcomeResolvedNotification {
		t.Fatalf("outcomes = %v", got)
	}
	if ch.sentCount() != 2 {
		t.Fatalf("want firing+resolution = 2 sends, got %d", ch.sentCount())
	}
}

func TestSilenceMutesButStateTracked(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := &testChannel{name: "mem"}
	rules := rule.NewMatchers([]rule.SilenceRule{{
		ID: "s1", Equal: map[string]string{"host": "n3"}, StartsAt: t0.Add(-time.Hour), EndsAt: t0.Add(time.Hour),
	}}, nil)
	e, _ := newTestEngine(t, fc, []notify.Channel{ch}, rules, notify.DefaultConfig())
	ctx := context.Background()
	ls := map[string]string{"alertname": "Disk", "host": "n3"}

	d := e.Ingest(ctx, firing("a", ls))
	logDecision(t, firing("a", ls), d)
	if d.Outcome != alert.OutcomeSilenced || d.RuleID != "s1" {
		t.Fatalf("want silenced by s1, got %s/%s", d.Outcome, d.RuleID)
	}
	d = e.Ingest(ctx, resolved("a", ls))
	logDecision(t, resolved("a", ls), d)
	if d.Outcome != alert.OutcomeResolvedNoNotification {
		t.Fatalf("muted alert resolution = %s, want resolved_no_notification", d.Outcome)
	}
	if ch.sentCount() != 0 {
		t.Fatal("silenced lifecycle produced notifications")
	}
	entry := e.State()["a"]
	if entry.Status != alert.StateResolved || !entry.Silenced {
		t.Fatalf("silenced state not tracked: %+v", entry)
	}
}

func TestInhibitionMutesTargetWhileSourceActive(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	rules := rule.NewMatchers(nil, []rule.InhibitRule{{
		ID: "i1", SourceEqual: map[string]string{"severity": "critical"},
		TargetEqual: map[string]string{"severity": "warning"}, Equal: []string{"host"},
	}})
	e, rec := newTestEngine(t, fc, []notify.Channel{&testChannel{name: "mem"}}, rules, notify.DefaultConfig())
	ctx := context.Background()

	// Warning arrives first with no critical source: notifies.
	warn := map[string]string{"alertname": "CPU", "host": "n1", "severity": "warning"}
	d := e.Ingest(ctx, firing("w", warn))
	logDecision(t, firing("w", warn), d)
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("warning without source = %s, want notified", d.Outcome)
	}

	// Critical source starts.
	crit := map[string]string{"alertname": "NodeDown", "host": "n1", "severity": "critical"}
	d = e.Ingest(ctx, firing("c", crit))
	logDecision(t, firing("c", crit), d)

	// Second warning on the same host is now inhibited: tracked, no notify.
	d = e.Ingest(ctx, firing("w2", warn))
	logDecision(t, firing("w2", warn), d)
	if d.Outcome != alert.OutcomeSuppressed || d.RuleID != "i1" {
		t.Fatalf("want suppressed by i1, got %s/%s", d.Outcome, d.RuleID)
	}

	// Warning on a different host is not inhibited.
	other := map[string]string{"alertname": "CPU", "host": "n2", "severity": "warning"}
	d = e.Ingest(ctx, firing("w3", other))
	logDecision(t, firing("w3", other), d)
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("cross-host warning = %s, want notified", d.Outcome)
	}

	if e.State()["w2"].Suppressed != true {
		t.Fatal("inhibited entry not marked suppressed")
	}
	_ = rec
}

func TestSilenceHasPriorityOverInhibition(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	rules := rule.NewMatchers(
		[]rule.SilenceRule{{ID: "s1", Equal: map[string]string{"host": "n1"}, StartsAt: t0.Add(-time.Hour), EndsAt: t0.Add(time.Hour)}},
		[]rule.InhibitRule{{ID: "i1", SourceEqual: map[string]string{"severity": "critical"}, TargetEqual: map[string]string{"severity": "warning"}, Equal: []string{"host"}}},
	)
	e, rec := newTestEngine(t, fc, []notify.Channel{&testChannel{name: "mem"}}, rules, notify.DefaultConfig())
	ctx := context.Background()

	d := e.Ingest(ctx, firing("c", map[string]string{"alertname": "Down", "host": "n1", "severity": "critical"}))
	logDecision(t, firing("c", map[string]string{"host": "n1"}), d)
	d = e.Ingest(ctx, firing("w", map[string]string{"alertname": "CPU", "host": "n1", "severity": "warning"}))
	logDecision(t, firing("w", map[string]string{"host": "n1"}), d)
	if d.Outcome != alert.OutcomeSilenced || d.RuleID != "s1" {
		t.Fatalf("want silence to win over inhibition, got %s/%s", d.Outcome, d.RuleID)
	}
	_ = rec
}

// failingNChannel fails the first failN sends, then succeeds.
type failingNChannel struct {
	mu      sync.Mutex
	name    string
	failN   int32
	deliver int
	attempt []int // attempts count on terminal results
}

func (c *failingNChannel) Name() string { return c.name }
func (c *failingNChannel) Send(ctx context.Context, n notify.Notification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failN > 0 {
		c.failN--
		return fmt.Errorf("simulated failure")
	}
	c.deliver++
	return nil
}

func ingestWait(t *testing.T, e *Engine, fc *clock.FakeClock, a alert.Alert) Decision {
	t.Helper()
	done := make(chan Decision, 1)
	go func() { done <- e.Ingest(context.Background(), a) }()
	var d Decision
	for {
		select {
		case d = <-done:
			logDecision(t, a, d)
			return d
		default:
			fc.Advance(time.Millisecond)
		}
	}
}

func TestReloadRulesChangesFutureConclusionsOnly(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := &testChannel{name: "mem"}
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{ch}, rule.Empty, Config{GroupBy: []string{"host"}}, dcfg, rec)
	defer e.Close()

	// a1 fires under empty ruleset: notified.
	d := ingestWait(t, e, fc, firing("a1", map[string]string{"host": "n1", "alertname": "X"}))
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("pre-reload a1 = %s, want notified", d.Outcome)
	}

	// Reload: silence host=n1 going forward.
	e.Reload(rule.NewMatchers([]rule.SilenceRule{{
		ID: "s1", Equal: map[string]string{"host": "n1"},
		StartsAt: t0.Add(-time.Hour), EndsAt: t0.Add(time.Hour),
	}}, nil))

	// New alert a2 now silenced (no notification), state still tracked.
	d = ingestWait(t, e, fc, firing("a2", map[string]string{"host": "n1", "alertname": "Y"}))
	if d.Outcome != alert.OutcomeSilenced {
		t.Fatalf("post-reload a2 = %s, want silenced", d.Outcome)
	}
	if !e.State()["a2"].Silenced {
		t.Fatal("a2 silenced state not tracked after reload")
	}
	// Repeat of the pre-reload already-notified alert remains a duplicate:
	// reload must not create a second notification.
	d = ingestWait(t, e, fc, firing("a1", map[string]string{"host": "n1", "alertname": "X"}))
	if d.Outcome != alert.OutcomeDuplicate {
		t.Fatalf("repeat after reload = %s, want duplicate (no duplicate notification)", d.Outcome)
	}
	if ch.sentCount() != 1 {
		t.Fatalf("reload changed delivery count: %d, want 1", ch.sentCount())
	}

	// Reload again dropping the silence: a subsequent muted-alert repeat
	// gets its first notification (latch had never fired).
	e.Reload(rule.Empty)
	d = ingestWait(t, e, fc, firing("a2", map[string]string{"host": "n1", "alertname": "Y"}))
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("after silence removed a2 = %s, want notified", d.Outcome)
	}
	if ch.sentCount() != 2 {
		t.Fatalf("want 2 deliveries total, got %d", ch.sentCount())
	}
}

func TestSilenceExpiryUnmutesFutureFiring(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	rules := rule.NewMatchers([]rule.SilenceRule{{
		ID: "s1", Equal: map[string]string{"host": "n1"},
		StartsAt: t0, EndsAt: t0.Add(time.Hour),
	}}, nil)
	e, _ := newTestEngine(t, fc, []notify.Channel{&testChannel{name: "mem"}}, rules,
		notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

	d := ingestWait(t, e, fc, firing("a", map[string]string{"host": "n1", "alertname": "X"}))
	if d.Outcome != alert.OutcomeSilenced {
		t.Fatalf("inside window = %s", d.Outcome)
	}
	// Advance past EndsAt; same condition repeats and now notifies.
	fc.Advance(2 * time.Hour)
	d = ingestWait(t, e, fc, firing("a", map[string]string{"host": "n1", "alertname": "X"}))
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("after silence expiry = %s, want notified", d.Outcome)
	}
}

func TestDeliveryFailureRetriedThenDeliveredWithoutStateChange(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	failing := &failingNChannel{name: "flaky", failN: 2} // fail twice, succeed 3rd
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 3, BaseBackoff: 2 * time.Millisecond, MaxBackoff: 10 * time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{failing}, rule.Empty, Config{}, dcfg, rec)
	defer e.Close()

	d := ingestWait(t, e, fc, firing("a", map[string]string{"host": "n1"}))
	if d.Outcome != alert.OutcomeNotified {
		t.Fatalf("outcome = %s", d.Outcome)
	}
	// Retries did not alter lifecycle state: still a single pending entry,
	// latch held, exactly one decision.
	entry := e.State()["a"]
	if entry.Status != alert.StatePending || !entry.Notified {
		t.Fatalf("state after retries = %+v", entry)
	}
	if len(rec.Decisions()) != 1 {
		t.Fatalf("retries produced extra decisions: %d", len(rec.Decisions()))
	}
	if failing.deliver != 1 {
		t.Fatalf("want exactly one successful delivery, got %d", failing.deliver)
	}
	r := rec.Results()[0]
	if !r.Delivered || r.Attempts != 3 {
		t.Fatalf("terminal result = %+v", r)
	}
}

func TestDeliveryExhaustionRecordedAsExhausted(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	dead := &failingNChannel{name: "dead", failN: 1 << 20}
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{dead}, rule.Empty, Config{}, dcfg, rec)
	defer e.Close()

	ingestWait(t, e, fc, firing("a", map[string]string{"host": "n1"}))
	r := rec.Results()[0]
	if r.Delivered || r.DropReason != notify.DropExhausted {
		t.Fatalf("want exhausted drop, got %+v", r)
	}
	if r.Attempts != 2 || r.Err == nil {
		t.Fatalf("attempts/err wrong: %+v", r)
	}
}

func TestConcurrentProducersNoDuplicates(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := &testChannel{name: "mem"}
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{ch}, rule.Empty, Config{GroupBy: []string{"host"}}, dcfg, rec)
	defer e.Close()

	const producers = 16
	const repeats = 25
	var wg sync.WaitGroup
	// Each producer hammers the SAME fingerprint; only one firing
	// notification may ever be produced regardless of interleaving.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < repeats; i++ {
				a := firing("same", map[string]string{"host": "n1", "alertname": "X", "i": fmt.Sprint(i)})
				d := e.Ingest(context.Background(), a)
				_ = d
			}
		}()
	}
	wg.Wait()
	settle(t, e, fc)

	var notified, duplicates int
	for _, d := range rec.Decisions() {
		switch d.Outcome {
		case alert.OutcomeNotified:
			notified++
		case alert.OutcomeDuplicate:
			duplicates++
		}
	}
	if notified != 1 {
		t.Fatalf("notified count = %d, want exactly 1", notified)
	}
	if duplicates != producers*repeats-1 {
		t.Fatalf("duplicate count = %d, want %d", duplicates, producers*repeats-1)
	}
	if ch.sentCount() != 1 {
		t.Fatalf("channel sends = %d, want 1", ch.sentCount())
	}
}
