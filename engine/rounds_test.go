package engine

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
	"github.com/h3yow/alert-routing-engine-go/clock"
	"github.com/h3yow/alert-routing-engine-go/notify"
	"github.com/h3yow/alert-routing-engine-go/rule"
)

// recChannel records every delivered notification (kind + round + group) in
// delivery order. All methods are safe for concurrent use.
type recChannel struct {
	mu   sync.Mutex
	name string
	log  []notify.Notification
}

func newRecChannel(name string) *recChannel { return &recChannel{name: name} }
func (c *recChannel) Name() string          { return c.name }
func (c *recChannel) Send(ctx context.Context, n notify.Notification) error {
	c.mu.Lock()
	c.log = append(c.log, n)
	c.mu.Unlock()
	return nil
}
func (c *recChannel) all() []notify.Notification {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]notify.Notification, len(c.log))
	copy(out, c.log)
	return out
}

// countRound counts deliveries to this channel for one (round, kind).
func (c *recChannel) countRound(round int, kind alert.EventKind) int {
	n := 0
	for _, d := range c.all() {
		if d.Round == round && d.Kind == kind {
			n++
		}
	}
	return n
}

// TestMultiRoundReopensAfterRecovery drives firing -> resolve -> firing ->
// resolve. The second episode is a NEW round: it must notify again, get a
// resolution notification again, and the state must expose per-round
// history without later rounds overwriting earlier ones.
func TestMultiRoundReopensAfterRecovery(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := newRecChannel("mem")
	e, rec := newTestEngine(t, fc, []notify.Channel{ch}, nil,
		notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	ctx := context.Background()
	ls := map[string]string{"alertname": "CPU", "host": "n1"}

	type step struct {
		a       alert.Alert
		outcome alert.Outcome
		round   int
	}
	steps := []step{
		{firing("a", ls), alert.OutcomeNotified, 1},
		{resolved("a", ls), alert.OutcomeResolvedNotification, 1},
		{firing("a", ls), alert.OutcomeNotified, 2},
		{firing("a", ls), alert.OutcomeDuplicate, 2}, // dup within round 2
		{resolved("a", ls), alert.OutcomeResolvedNotification, 2},
		{resolved("a", ls), alert.OutcomeDuplicate, 2}, // dup recovery
		{firing("a", ls), alert.OutcomeNotified, 3},
		{resolved("a", ls), alert.OutcomeResolvedNotification, 3},
	}
	for _, s := range steps {
		fc.Advance(time.Second)
		d := e.Ingest(ctx, s.a)
		logDecision(t, s.a, d)
		if d.Outcome != s.outcome || d.Round != s.round {
			t.Fatalf("step kind=%s: got round=%d outcome=%s, want round=%d outcome=%s",
				s.a.Kind, d.Round, d.Outcome, s.round, s.outcome)
		}
	}

	// Each channel gets exactly one firing + one resolution per round.
	for r := 1; r <= 3; r++ {
		if got := ch.countRound(r, alert.KindFiring); got != 1 {
			t.Fatalf("round %d firing deliveries = %d, want 1", r, got)
		}
		if got := ch.countRound(r, alert.KindResolved); got != 1 {
			t.Fatalf("round %d resolution deliveries = %d, want 1", r, got)
		}
	}
	if got := len(ch.all()); got != 6 {
		t.Fatalf("total deliveries = %d, want 6 (3 rounds x firing+resolve)", got)
	}

	// Delivery order matches state evolution within each round.
	got := ch.all()
	wantKinds := []struct {
		round int
		kind  alert.EventKind
	}{
		{1, alert.KindFiring}, {1, alert.KindResolved},
		{2, alert.KindFiring}, {2, alert.KindResolved},
		{3, alert.KindFiring}, {3, alert.KindResolved},
	}
	for i, w := range wantKinds {
		if got[i].Round != w.round || got[i].Kind != w.kind {
			t.Fatalf("delivery[%d] = r%d/%s, want r%d/%s", i, got[i].Round, got[i].Kind, w.round, w.kind)
		}
	}

	// State shows the current round and full history with its own latches.
	entry := e.State()["a"]
	if entry.RoundNo != 3 || entry.Status != alert.StateResolved {
		t.Fatalf("state = round %d status %s, want round 3 resolved", entry.RoundNo, entry.Status)
	}
	if len(entry.Rounds) != 3 {
		t.Fatalf("history rounds = %d, want 3", len(entry.Rounds))
	}
	for i, r := range entry.Rounds {
		if r.Number != i+1 || !r.FiringNotified || !r.ResolveNotified || r.Open() {
			t.Fatalf("round %+v not closed with both latches set", r)
		}
		if r.FiredAt.IsZero() || r.ResolvedAt.IsZero() || !r.FiredAt.Before(r.ResolvedAt) {
			t.Fatalf("round %d timing wrong: fired=%v resolved=%v", r.Number, r.FiredAt, r.ResolvedAt)
		}
	}
	// Earlier rounds must not be overwritten by the latest payload-less flag.
	if entry.Rounds[0].Number != 1 || entry.Rounds[1].Number != 2 {
		t.Fatalf("history order corrupted: %+v", entry.Rounds)
	}
	_ = rec
}

// TestOrphanAndOutOfOrderSurviveMultiRound pins the compatibility edge:
// orphan recovery, duplicate recovery and out-of-order firing keep distinct
// conclusions and are not turned into new rounds.
func TestOrphanAndOutOfOrderSurviveMultiRound(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := newRecChannel("mem")
	e, rec := newTestEngine(t, fc, []notify.Channel{ch}, nil,
		notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	ctx := context.Background()
	ls := map[string]string{"alertname": "Ghost", "host": "n9"}

	ingest := func(a alert.Alert, want alert.Outcome, wantRound int) {
		t.Helper()
		d := e.Ingest(ctx, a)
		logDecision(t, a, d)
		if d.Outcome != want || d.Round != wantRound {
			t.Fatalf("kind=%s got r%d/%s, want r%d/%s", a.Kind, d.Round, d.Outcome, wantRound, want)
		}
	}

	ingest(resolved("g", ls), alert.OutcomeOrphanResolve, 0)
	ingest(resolved("g", ls), alert.OutcomeDuplicate, 0)
	ingest(firing("g", ls), alert.OutcomeOutOfOrder, 0)
	ingest(firing("g", ls), alert.OutcomeOutOfOrder, 0)

	if e.State()["g"].OrphanResolved != true || len(e.State()["g"].Rounds) != 0 {
		t.Fatalf("orphan entry must keep no rounds: %+v", e.State()["g"])
	}
	if len(ch.all()) != 0 {
		t.Fatalf("orphan path produced %d deliveries", len(ch.all()))
	}
	_ = rec
}

// TestRound2EvaluatedAgainstReloadedRules verifies a follow-up round is a
// fresh judgment against the rules/state at its own arrival time; reload
// neither revokes round 1's in-flight conclusions nor pre-mutes round 2.
func TestRound2EvaluatedAgainstReloadedRules(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := newRecChannel("mem")
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{ch}, rule.Empty,
		Config{GroupBy: []string{"alertname", "host"}}, dcfg, rec)
	defer e.Close()
	ctx := context.Background()
	ls := map[string]string{"alertname": "CPU", "host": "n1"}

	d := e.Ingest(ctx, firing("a", ls))
	logDecision(t, firing("a", ls), d)
	d = e.Ingest(ctx, resolved("a", ls))
	logDecision(t, resolved("a", ls), d)
	if ch.countRound(1, alert.KindFiring) != 1 || ch.countRound(1, alert.KindResolved) != 1 {
		t.Fatal("round 1 must have delivered firing+resolution")
	}

	// Reload adds a silence BEFORE round 2 starts.
	e.Reload(rule.NewMatchers([]rule.SilenceRule{{
		ID: "s1", Equal: map[string]string{"host": "n1"},
		StartsAt: t0.Add(-time.Hour), EndsAt: t0.Add(10 * time.Hour),
	}}, nil))

	d = e.Ingest(ctx, firing("a", ls))
	logDecision(t, firing("a", ls), d)
	if d.Outcome != alert.OutcomeSilenced || d.Round != 2 || d.RuleID != "s1" {
		t.Fatalf("round 2 under new silence = r%d/%s/%s, want r2/silenced/s1", d.Round, d.Outcome, d.RuleID)
	}
	// Resolve while muted whole life: no resolution notification for r2.
	d = e.Ingest(ctx, resolved("a", ls))
	logDecision(t, resolved("a", ls), d)
	if d.Outcome != alert.OutcomeResolvedNoNotification || d.Round != 2 {
		t.Fatalf("muted round 2 resolve = r%d/%s", d.Round, d.Outcome)
	}
	// No deliveries at all for round 2; round 1 deliveries untouched.
	if ch.countRound(2, alert.KindFiring) != 0 || ch.countRound(2, alert.KindResolved) != 0 {
		t.Fatal("muted round 2 must not deliver anything")
	}
	if len(ch.all()) != 2 {
		t.Fatalf("reload disturbed round 1 deliveries: %d total", len(ch.all()))
	}

	// Remove the silence; round 3 notifies again on its own merits.
	e.Reload(rule.Empty)
	fc.Advance(time.Hour)
	d = e.Ingest(ctx, firing("a", ls))
	logDecision(t, firing("a", ls), d)
	if d.Outcome != alert.OutcomeNotified || d.Round != 3 {
		t.Fatalf("round 3 after silence removed = r%d/%s", d.Round, d.Outcome)
	}
	d = e.Ingest(ctx, resolved("a", ls))
	logDecision(t, resolved("a", ls), d)
	if d.Outcome != alert.OutcomeResolvedNotification || d.Round != 3 {
		t.Fatalf("round 3 resolve = r%d/%s", d.Round, d.Outcome)
	}
	if ch.countRound(3, alert.KindFiring) != 1 || ch.countRound(3, alert.KindResolved) != 1 {
		t.Fatal("round 3 must deliver firing + resolution")
	}
	if got := len(rec.Decisions()); got != 6 {
		t.Fatalf("decision count = %d, want 6", got)
	}
}

// TestMultiRoundRetriesDoNotDuplicate confirms failure retry inside a round
// still yields exactly one successful send per kind per channel and does
// not touch the state machine.
func TestMultiRoundRetriesDoNotDuplicate(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	failing := &failingNChannel{name: "flaky", failN: 2}
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 3, BaseBackoff: 2 * time.Millisecond, MaxBackoff: 10 * time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{failing}, rule.Empty, Config{}, dcfg, rec)
	defer e.Close()
	ls := map[string]string{"host": "n1"}

	d := ingestWait(t, e, fc, firing("a", ls))
	if d.Outcome != alert.OutcomeNotified || d.Round != 1 {
		t.Fatalf("round 1 firing = r%d/%s", d.Round, d.Outcome)
	}
	d = ingestWait(t, e, fc, resolved("a", ls))
	if d.Outcome != alert.OutcomeResolvedNotification || d.Round != 1 {
		t.Fatalf("round 1 resolve = r%d/%s", d.Round, d.Outcome)
	}
	d = ingestWait(t, e, fc, firing("a", ls))
	if d.Outcome != alert.OutcomeNotified || d.Round != 2 {
		t.Fatalf("round 2 firing = r%d/%s", d.Round, d.Outcome)
	}
	if failing.deliver != 3 {
		t.Fatalf("successful sends = %d, want 3 (r1 fire, r1 resolve, r2 fire)", failing.deliver)
	}
	entry := e.State()["a"]
	if entry.Status != alert.StatePending || entry.RoundNo != 2 {
		t.Fatalf("state = %s round %d, want pending round 2", entry.Status, entry.RoundNo)
	}
	if len(rec.Decisions()) != 3 {
		t.Fatalf("retries produced extra decisions: %d", len(rec.Decisions()))
	}
}

// slowRecChannel records deliveries, tracks peak concurrency within one
// group, and optionally yields to widen the race window (no real sleeping).
type slowRecChannel struct {
	recChannel
	active   int32
	maxAct   int32
	inGroup  map[string]int32
	maxGroup map[string]int32
}

func newSlowRecChannel(name string) *slowRecChannel {
	return &slowRecChannel{
		recChannel: recChannel{name: name},
		inGroup:    map[string]int32{},
		maxGroup:   map[string]int32{},
	}
}

func (c *slowRecChannel) Send(ctx context.Context, n notify.Notification) error {
	c.mu.Lock()
	c.inGroup[n.Group]++
	if c.inGroup[n.Group] > c.maxGroup[n.Group] {
		c.maxGroup[n.Group] = c.inGroup[n.Group]
	}
	c.mu.Unlock()

	a := atomic.AddInt32(&c.active, 1)
	for {
		old := atomic.LoadInt32(&c.maxAct)
		if a <= old || atomic.CompareAndSwapInt32(&c.maxAct, old, a) {
			break
		}
	}
	runtime.Gosched() // yield only; determinism comes from the fake clock
	atomic.AddInt32(&c.active, -1)

	c.mu.Lock()
	c.log = append(c.log, n)
	c.inGroup[n.Group]--
	c.mu.Unlock()
	return nil
}
func (c *slowRecChannel) peakGroup(g string) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxGroup[g]
}

// TestConcurrentMultiRoundMultiChannel hammers one fingerprint with
// interleaved firing/recovery events from many producers across multiple
// channels. Producers synchronize on per-round barriers: within one round
// many firings race (exactly one notifies, the rest duplicate) and then
// many recoveries race (exactly one closes the round); regardless of
// interleaving each channel must receive exactly one firing and one
// resolution per round, in state-evolution order, with no concurrent
// deliveries within a group.
func TestConcurrentMultiRoundMultiChannel(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	c1 := newSlowRecChannel("ch-a")
	c2 := newSlowRecChannel("ch-b")
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{c1, c2}, rule.Empty,
		Config{GroupBy: []string{"host"}}, dcfg, rec)
	defer e.Close()
	ls := map[string]string{"host": "g1", "alertname": "Flap"}

	const producers = 12
	const nRounds = 10
	var wg sync.WaitGroup
	for r := 1; r <= nRounds; r++ {
		// Phase A: all producers fire concurrently, then wait for everyone.
		var fired sync.WaitGroup
		fired.Add(producers)
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer fired.Done()
				d := e.Ingest(context.Background(), firing("fp1", ls))
				logDecision(t, firing("fp1", ls), d)
			}()
		}
		fired.Wait()

		// Phase B: all producers recover concurrently.
		var recWg sync.WaitGroup
		recWg.Add(producers)
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer recWg.Done()
				d := e.Ingest(context.Background(), resolved("fp1", ls))
				logDecision(t, resolved("fp1", ls), d)
			}()
		}
		recWg.Wait()
	}
	wg.Wait()

	entry := e.State()["fp1"]
	if entry.Status != alert.StateResolved {
		t.Fatalf("state = %s, want resolved", entry.Status)
	}
	if entry.OrphanResolved {
		t.Fatal("entry incorrectly marked orphan")
	}
	if got := len(entry.Rounds); got != nRounds {
		t.Fatalf("rounds = %d, want %d", got, nRounds)
	}
	for _, r := range entry.Rounds {
		if r.Open() || !r.FiringNotified || !r.ResolveNotified {
			t.Fatalf("round %d not cleanly closed/notified: %+v", r.Number, r)
		}
	}

	for _, c := range []*slowRecChannel{c1, c2} {
		log := c.all()
		if len(log) != 2*nRounds {
			t.Fatalf("channel %s deliveries = %d, want %d", c.Name(), len(log), 2*nRounds)
		}
		// Per-round exactly-once for each kind.
		counts := map[int]map[alert.EventKind]int{}
		for _, n := range log {
			if counts[n.Round] == nil {
				counts[n.Round] = map[alert.EventKind]int{}
			}
			counts[n.Round][n.Kind]++
		}
		for r := 1; r <= nRounds; r++ {
			if counts[r][alert.KindFiring] != 1 || counts[r][alert.KindResolved] != 1 {
				t.Fatalf("channel %s round %d counts = firing:%d resolve:%d, want 1/1",
					c.Name(), r, counts[r][alert.KindFiring], counts[r][alert.KindResolved])
			}
		}
		// Ordering: firing of r before resolution of r, and resolution of r
		// before firing of r+1 (matches state evolution exactly).
		lastRound := 0
		lastKind := alert.KindResolved
		for i, n := range log {
			switch {
			case n.Round < lastRound:
				t.Fatalf("channel %s delivery %d went backwards: r%d after r%d", c.Name(), i, n.Round, lastRound)
			case n.Round == lastRound:
				if !(lastKind == alert.KindFiring && n.Kind == alert.KindResolved) {
					t.Fatalf("channel %s bad intra-round order at %d: %s then r%d/%s",
						c.Name(), i, lastKind, n.Round, n.Kind)
				}
			case n.Round == lastRound+1:
				if n.Kind != alert.KindFiring {
					t.Fatalf("channel %s round %d did not start with firing", c.Name(), n.Round)
				}
			default:
				t.Fatalf("channel %s skipped rounds: r%d after r%d", c.Name(), n.Round, lastRound)
			}
			lastRound, lastKind = n.Round, n.Kind
		}
		if peak := c.peakGroup("g1"); peak > 1 {
			t.Fatalf("channel %s peak concurrent deliveries in group = %d, want <=1", c.Name(), peak)
		}
	}

	// Cross-check decisions: every produced notification maps to a
	// notified/resolved_notification decision; counts match, and all the
	// other racing events are explicit duplicates (never extra rounds).
	var fireDec, resolveDec, dupDec int
	for _, d := range rec.Decisions() {
		switch d.Outcome {
		case alert.OutcomeNotified:
			fireDec++
		case alert.OutcomeResolvedNotification:
			resolveDec++
		case alert.OutcomeDuplicate:
			dupDec++
		}
	}
	if fireDec != nRounds || resolveDec != nRounds {
		t.Fatalf("notifying decisions = firing:%d resolve:%d, want %d/%d", fireDec, resolveDec, nRounds, nRounds)
	}
	if wantDup := 2 * nRounds * (producers - 1); dupDec != wantDup {
		t.Fatalf("duplicate decisions = %d, want %d", dupDec, wantDup)
	}
}

// TestConcurrentDistinctGroupsStayParallel confirms one group blocking
// delivery does not stall another group: two groups are ingested from
// concurrent producers and both complete with the exact per-round counts.
func TestConcurrentDistinctGroupsStayParallel(t *testing.T) {
	fc := clock.NewFakeClock(t0)
	ch := newSlowRecChannel("mem")
	rec := NewRecorder()
	dcfg := notify.Config{MaxAttempts: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	e := NewWithDispatchConfig(fc, []notify.Channel{ch}, rule.Empty,
		Config{GroupBy: []string{"host"}}, dcfg, rec)
	defer e.Close()

	const roundsN = 25
	var wg sync.WaitGroup
	for _, host := range []string{"h1", "h2"} {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			ls := map[string]string{"host": host}
			for r := 0; r < roundsN; r++ {
				e.Ingest(context.Background(), firing("fp-"+host, ls))
				e.Ingest(context.Background(), resolved("fp-"+host, ls))
			}
		}(host)
	}
	wg.Wait()

	for _, host := range []string{"h1", "h2"} {
		var firingN, resolveN int
		for _, n := range ch.all() {
			if n.Group != `"host"=`+strconv.Quote(host) {
				continue
			}
			if n.Kind == alert.KindFiring {
				firingN++
			} else {
				resolveN++
			}
		}
		if firingN != roundsN || resolveN != roundsN {
			t.Fatalf("group %s counts = firing:%d resolve:%d, want %d/%d", host, firingN, resolveN, roundsN, roundsN)
		}
		if peak := ch.peakGroup(`"host"=` + strconv.Quote(host)); peak > 1 {
			t.Fatalf("group %s peak concurrency = %d", host, peak)
		}
	}
	if len(ch.all()) != 4*roundsN {
		t.Fatalf("total deliveries = %d, want %d", len(ch.all()), 4*roundsN)
	}
}
