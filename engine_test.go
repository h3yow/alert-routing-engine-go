package alertrouter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var errFakeDelivery = errors.New("fake delivery failure")

// fakeNotifier 内存本地通知通道：记录每次投递，可按 (分组,状态) 配置前若干次失败，
// 并检测同一分组是否出现并发重叠投递（不允许）。
type fakeNotifier struct {
	id      string
	mu      sync.Mutex
	got     []Notification
	active  map[string]int // groupKey -> 正在执行的投递数
	overlap bool
	fails   map[string]int // key: groupKey#status -> 还要失败的次数
}

func newFakeNotifier(id string) *fakeNotifier {
	return &fakeNotifier{id: id, active: map[string]int{}, fails: map[string]int{}}
}

func (f *fakeNotifier) ID() string { return f.id }

func (f *fakeNotifier) Notify(_ context.Context, n Notification) error {
	f.mu.Lock()
	if f.active[n.GroupKey] > 0 {
		f.overlap = true // 同一分组出现并发投递，违反串行不变量
	}
	f.active[n.GroupKey]++
	f.got = append(f.got, n)
	key := fmt.Sprintf("%s#%d", n.GroupKey, n.Status)
	fail := f.fails[key] > 0
	if fail {
		f.fails[key]--
	}
	f.mu.Unlock()

	f.mu.Lock()
	f.active[n.GroupKey]--
	f.mu.Unlock()
	if fail {
		return errFakeDelivery
	}
	return nil
}

func (f *fakeNotifier) failFirst(key string, times int) {
	f.fails[key] = times
}

func (f *fakeNotifier) deliveries() []Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Notification, len(f.got))
	copy(out, f.got)
	return out
}

type harness struct {
	t   *testing.T
	e   *Engine
	clk *fakeClock
	nts []*fakeNotifier
}

func newHarness(t *testing.T, cfg Config, nts ...*fakeNotifier) *harness {
	t.Helper()
	clk := newFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	all := make([]Notifier, len(nts))
	for i, n := range nts {
		all[i] = n
	}
	return &harness{t: t, e: NewEngine(cfg, all, clk), clk: clk, nts: nts}
}

func (h *harness) submit(ev AlertEvent) Decision {
	d := h.e.Submit(ev)
	h.t.Logf("输入 event{fp=%s labels=%v status=%d} => 判定 reason=%s group=%q rule=%q",
		ev.Fingerprint, ev.Labels, ev.Status, d.Reason, d.GroupKey, d.RuleName)
	return d
}

func firingEvent(fp string, labels LabelSet) AlertEvent {
	return AlertEvent{Fingerprint: fp, Labels: labels, Status: EventFiring,
		StartsAt: 1_700_000_000_000}
}

func resolvedEvent(fp string, labels LabelSet) AlertEvent {
	return AlertEvent{Fingerprint: fp, Labels: labels, Status: EventResolved,
		StartsAt: 1_700_000_000_000, EndsAt: 1_700_000_060_000}
}

// TestDedupRepeatedFiring 同一条触发重复到达只能通知一次。
func TestDedupRepeatedFiring(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service", "severity"}}, n)
	defer h.e.Close()

	labels := LabelSet{"service": "api", "severity": "page"}
	ev := firingEvent("fp-1", labels)

	d1 := h.submit(ev)
	if d1.Reason != ReasonNotified {
		t.Fatalf("首次触发期望 notified，实际 %s", d1.Reason)
	}
	for i := 0; i < 3; i++ {
		d := h.submit(ev)
		if d.Reason != ReasonDuplicate {
			t.Fatalf("第 %d 次重复触发期望 duplicate，实际 %s", i+2, d.Reason)
		}
	}
	h.e.waitSettled()
	if got := n.deliveries(); len(got) != 1 {
		t.Fatalf("重复触发不应重复投递，期望 1 条投递，实际 %d: %+v", len(got), got)
	}
}

// TestOrphanResolve 孤立恢复：没有对应触发，不落状态、不通知，且与乱序恢复结论不同。
func TestOrphanResolve(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}}, n)
	defer h.e.Close()

	d := h.submit(resolvedEvent("fp-x", LabelSet{"service": "db"}))
	if d.Reason != ReasonOrphanResolve {
		t.Fatalf("孤立恢复期望 orphan_resolve，实际 %s", d.Reason)
	}
	h.e.waitSettled()
	if len(n.deliveries()) != 0 {
		t.Fatalf("孤立恢复不应投递，实际 %+v", n.deliveries())
	}

	// 孤立恢复之后真正的触发到达，仍按新触发处理（状态没有被孤立恢复污染）。
	d2 := h.submit(firingEvent("fp-x", LabelSet{"service": "db"}))
	if d2.Reason != ReasonNotified {
		t.Fatalf("孤立恢复之后的触发期望 notified，实际 %s", d2.Reason)
	}
}

// TestNormalResolveAndOutOfOrder 正常恢复对应上触发；已恢复后再来恢复是乱序过期重复。
func TestNormalResolveAndOutOfOrder(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}}, n)
	defer h.e.Close()

	labels := LabelSet{"service": "db"}
	if d := h.submit(firingEvent("fp-1", labels)); d.Reason != ReasonNotified {
		t.Fatalf("触发期望 notified，实际 %s", d.Reason)
	}
	h.e.waitSettled()

	if d := h.submit(resolvedEvent("fp-1", labels)); d.Reason != ReasonResolved {
		t.Fatalf("正常恢复期望 resolved，实际 %s", d.Reason)
	}
	h.e.waitSettled()

	// 乱序/过期重复恢复：结论必须是 out_of_order，而不是 orphan_resolve。
	if d := h.submit(resolvedEvent("fp-1", labels)); d.Reason != ReasonOutOfOrder {
		t.Fatalf("过期重复恢复期望 out_of_order，实际 %s（不得与孤立恢复混淆）", d.Reason)
	}
	h.e.waitSettled()

	// 恢复后再次触发：新一轮事故，可以再次通知。
	if d := h.submit(firingEvent("fp-1", labels)); d.Reason != ReasonNotified {
		t.Fatalf("恢复后再触发期望 notified，实际 %s", d.Reason)
	}
	h.e.waitSettled()

	got := n.deliveries()
	if len(got) != 3 {
		t.Fatalf("期望 firing/resolved/firing 共 3 条投递，实际 %d: %+v", len(got), got)
	}
	if got[0].Status != EventFiring || got[1].Status != EventResolved || got[2].Status != EventFiring {
		t.Fatalf("投递状态序列错误: %+v", got)
	}
}

// TestInhibit 抑制源触发期间，目标被抑制，不通知但状态仍被跟踪（可被后续恢复）。
func TestInhibit(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}}, n)
	defer h.e.Close()
	h.e.ReloadRules(RuleSet{Inhibits: []InhibitRule{{
		Name:   "node-down-masks-pod",
		Source: LabelSet{"alert": "NodeDown"},
		Target: LabelSet{"alert": "PodRestart"},
		Equal:  []string{"node"},
	}}})

	// 先到源告警（NodeDown）
	if d := h.submit(firingEvent("fp-node", LabelSet{"service": "db", "alert": "NodeDown", "node": "n1"})); d.Reason != ReasonNotified {
		t.Fatalf("抑制源期望 notified，实际 %s", d.Reason)
	}
	// 目标在源触发期间到达，且 node 相等 -> 被抑制
	d := h.submit(firingEvent("fp-pod", LabelSet{"service": "db", "alert": "PodRestart", "node": "n1"}))
	if d.Reason != ReasonSuppressed || d.RuleName != "node-down-masks-pod" {
		t.Fatalf("期望被规则 node-down-masks-pod 抑制，实际 reason=%s rule=%s", d.Reason, d.RuleName)
	}
	// equal 标签不一致则不抑制（放到不同分组，避免被分组投递合并）
	d2 := h.submit(firingEvent("fp-pod2", LabelSet{"service": "web", "alert": "PodRestart", "node": "n2"}))
	if d2.Reason != ReasonNotified {
		t.Fatalf("equal 标签不一致不应被抑制，实际 %s", d2.Reason)
	}
	// 被抑制的目标状态仍被跟踪：其恢复能对应上（正常恢复，而非孤立恢复）
	d3 := h.submit(resolvedEvent("fp-pod", LabelSet{"service": "db", "alert": "PodRestart", "node": "n1"}))
	if d3.Reason != ReasonSuppressed {
		t.Fatalf("抑制期间目标恢复仍应被抑制，实际 %s", d3.Reason)
	}
}

// TestSilence 静默告警不通知但状态正常跟踪。
func TestSilence(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}}, n)
	defer h.e.Close()
	h.e.ReloadRules(RuleSet{Silences: []SilenceRule{{
		Name: "maintenance", Match: LabelSet{"env": "test"},
	}}})

	d := h.submit(firingEvent("fp-1", LabelSet{"service": "api", "env": "test"}))
	if d.Reason != ReasonSilenced || d.RuleName != "maintenance" {
		t.Fatalf("期望静默 maintenance，实际 reason=%s rule=%s", d.Reason, d.RuleName)
	}
	if d2 := h.submit(resolvedEvent("fp-1", LabelSet{"service": "api", "env": "test"})); d2.Reason != ReasonSilenced {
		t.Fatalf("静默告警状态应被跟踪，恢复仍判静默，实际 %s", d2.Reason)
	}
	h.e.waitSettled()
	if len(n.deliveries()) != 0 {
		t.Fatalf("静默期间不应有投递，实际 %+v", n.deliveries())
	}

	// 未命中静默的同服务其他告警照常通知
	if d3 := h.submit(firingEvent("fp-2", LabelSet{"service": "api", "env": "prod"})); d3.Reason != ReasonNotified {
		t.Fatalf("未命中静默应通知，实际 %s", d3.Reason)
	}
}

// TestSilencePriority 同时命中静默和抑制时，结论是静默（静默优先级更高）。
func TestSilencePriority(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}}, n)
	defer h.e.Close()
	h.e.ReloadRules(RuleSet{
		Silences: []SilenceRule{{Name: "mute", Match: LabelSet{"env": "test"}}},
		Inhibits: []InhibitRule{{Name: "inh", Source: LabelSet{"kind": "src"},
			Target: LabelSet{"kind": "tgt"}}},
	})
	h.submit(firingEvent("fp-src", LabelSet{"service": "s", "kind": "src", "env": "test"}))
	d := h.submit(firingEvent("fp-tgt", LabelSet{"service": "s", "kind": "tgt", "env": "test"}))
	if d.Reason != ReasonSilenced {
		t.Fatalf("静默与抑制同时命中时期望 silenced，实际 %s", d.Reason)
	}
}

// TestReloadRules 在途投递不被重载取消，规则只对重载后的判定生效，状态不丢、不重复通知。
func TestReloadRules(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}, MaxAttempts: 1,
		BaseBackoff: time.Millisecond}, n)
	defer h.e.Close()

	// 1) 无规则：触发进入投递
	d := h.submit(firingEvent("fp-1", LabelSet{"service": "api", "env": "prod"}))
	if d.Reason != ReasonNotified {
		t.Fatalf("重载前期望 notified，实际 %s", d.Reason)
	}
	// 2) 运行中加载静默规则
	h.e.ReloadRules(RuleSet{Silences: []SilenceRule{{Name: "mute-prod", Match: LabelSet{"env": "prod"}}}})
	// 3) 已在途的触发投递照常完成，不因重载丢失
	h.e.waitSettled()
	if len(n.deliveries()) != 1 {
		t.Fatalf("在途投递不应被重载取消，期望 1 条，实际 %d", len(n.deliveries()))
	}
	// 4) 重载后的新判定按新规则：重复触发是 duplicate（状态未丢），恢复被静默
	if d2 := h.submit(firingEvent("fp-1", LabelSet{"service": "api", "env": "prod"})); d2.Reason != ReasonDuplicate {
		t.Fatalf("重载不应丢失已有告警状态，重复触发期望 duplicate，实际 %s", d2.Reason)
	}
	if d3 := h.submit(resolvedEvent("fp-1", LabelSet{"service": "api", "env": "prod"})); d3.Reason != ReasonSilenced {
		t.Fatalf("重载后的恢复期望按新规则静默，实际 %s", d3.Reason)
	}
	// 5) 再热更新移除规则：再次触发（新一轮）恢复通知
	h.e.ReloadRules(RuleSet{})
	if d4 := h.submit(firingEvent("fp-2", LabelSet{"service": "api", "env": "prod"})); d4.Reason != ReasonNotified {
		t.Fatalf("移除规则后新告警期望 notified，实际 %s", d4.Reason)
	}
	h.e.waitSettled()
	if got := n.deliveries(); len(got) != 2 {
		t.Fatalf("重载全过程不应产生重复通知，期望 2 条投递，实际 %d: %+v", len(got), got)
	}
}

// TestDeliveryRetrySuccess 投递先失败、退避后重试成功；退避由假时钟确定性推进。
func TestDeliveryRetrySuccess(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}, MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond}, n)
	defer h.e.Close()
	n.failFirst("service=api#0", 2)

	d := h.submit(firingEvent("fp-1", LabelSet{"service": "api"}))
	if d.Reason != ReasonNotified {
		t.Fatalf("期望 notified，实际 %s", d.Reason)
	}
	h.clk.waitPending(1) // 第 1 次失败，第 2 次尝试的退避定时器已登记
	if got := n.deliveries(); len(got) != 1 {
		t.Fatalf("失败尝试也应到达通道，期望 1 次调用，实际 %d", len(got))
	}
	h.clk.Add(10 * time.Millisecond) // 触发第 2 次尝试（仍失败）
	h.clk.waitPending(1)
	h.clk.Add(20 * time.Millisecond) // 触发第 3 次尝试（成功）
	h.e.waitSettled()

	if got := n.deliveries(); len(got) != 3 {
		t.Fatalf("期望 3 次尝试后成功，实际投递调用 %d", len(got))
	}
	rs := h.e.DrainResults()
	if len(rs) != 1 || !rs[0].Delivered || rs[0].Attempts != 3 {
		t.Fatalf("期望第 3 次尝试成功，实际 %+v", rs)
	}
	// 重试期间告警状态不受影响：恢复判定仍正常对应
	if d2 := h.submit(resolvedEvent("fp-1", LabelSet{"service": "api"})); d2.Reason != ReasonResolved {
		t.Fatalf("重试不应扰乱状态流转，恢复期望 resolved，实际 %s", d2.Reason)
	}
}

// TestDeliveryDroppedAfterRetries 重试耗尽 = 投递失败丢弃，与主动合并丢弃原因不同。
func TestDeliveryDroppedAfterRetries(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}, MaxAttempts: 2,
		BaseBackoff: 10 * time.Millisecond}, n)
	defer h.e.Close()
	n.failFirst("service=api#0", 5) // 一直失败

	d := h.submit(firingEvent("fp-1", LabelSet{"service": "api"}))
	if d.Reason != ReasonNotified {
		t.Fatalf("入队时仍应返回 notified，实际 %s", d.Reason)
	}
	h.clk.waitPending(1)
	h.clk.Add(10 * time.Millisecond) // 触发第 2 次尝试（仍失败，重试耗尽）
	h.e.waitSettled()

	rs := h.e.DrainResults()
	if len(rs) != 1 || rs[0].Delivered || rs[0].Attempts != 2 || rs[0].Err == nil {
		t.Fatalf("期望 2 次尝试后投递失败丢弃，实际 %+v", rs)
	}
	t.Logf("投递失败结论: %+v err=%v（对应 Reason=ReasonDeliveryDropped）", rs[0], rs[0].Err)

	// 主动合并丢弃是同步结论 ReasonCoalesced，二者原因互不相同：
	// 见 TestConcurrentCoalesce。
}

// TestConcurrentCoalesce 同一分组相同状态的并发提交，最多投递一次，其余为主动合并丢弃。
func TestConcurrentCoalesced(t *testing.T) {
	n := newFakeNotifier("local-1")
	h := newHarness(t, Config{GroupBy: []string{"service"}, MaxAttempts: 1}, n)
	defer h.e.Close()

	var wg sync.WaitGroup
	reasons := make([]Reason, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reasons[i] = h.submit(firingEvent("fp-same", LabelSet{"service": "api"})).Reason
		}(i)
	}
	wg.Wait()

	var notified, dup, coalesced int
	for _, r := range reasons {
		switch r {
		case ReasonNotified:
			notified++
		case ReasonDuplicate:
			dup++
		case ReasonCoalesced:
			coalesced++
		}
	}
	t.Logf("并发结论分布 notified=%d duplicate=%d coalesced=%d", notified, dup, coalesced)
	if notified != 1 {
		t.Fatalf("同分组同状态应只有 1 次进入投递，实际 %d", notified)
	}
	if dup+coalesced != 7 {
		t.Fatalf("其余 7 次应是状态去重或投递合并丢弃，实际 duplicate=%d coalesced=%d", dup, coalesced)
	}
	h.e.waitSettled()
	if got := n.deliveries(); len(got) != 1 {
		t.Fatalf("同一分组不得并发重复投递，期望 1 条，实际 %d", len(got))
	}
}

// TestConcurrentProducersAndChannels 多生产者并发、多通道并发投递，-race 下无重复投递。
func TestConcurrentProducersAndChannels(t *testing.T) {
	n1 := newFakeNotifier("ch-a")
	n2 := newFakeNotifier("ch-b")
	n3 := newFakeNotifier("ch-c")
	h := newHarness(t, Config{GroupBy: []string{"service"}, MaxAttempts: 1}, n1, n2, n3)
	defer h.e.Close()

	const producers = 4
	const alertsPerProducer = 25
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < alertsPerProducer; i++ {
				fp := fmt.Sprintf("fp-%d-%d", p, i)
				labels := LabelSet{"service": fmt.Sprintf("svc-%d", i%5)}
				h.submit(firingEvent(fp, labels))
			}
		}(p)
	}
	wg.Wait()
	h.e.waitSettled()

	totalFiring := producers * alertsPerProducer
	for _, n := range h.nts {
		got := n.deliveries()
		if n.overlap {
			t.Fatalf("通道 %s 同一分组出现并发重叠投递", n.id)
		}
		// 不变量 1：每条告警（指纹）在每个通道恰好投递一次，不重复。
		seen := map[string]int{}
		for _, d := range got {
			if d.Status != EventFiring {
				continue
			}
			for _, a := range d.Alerts {
				seen[a.Fingerprint]++
			}
		}
		if len(seen) != totalFiring {
			t.Fatalf("通道 %s 期望覆盖 %d 条不同告警，实际 %d", n.id, totalFiring, len(seen))
		}
		for fp, c := range seen {
			if c != 1 {
				t.Fatalf("通道 %s 告警 %s 被重复投递 %d 次", n.id, fp, c)
			}
		}
		// 不变量 2：所有投递都落在 5 个配置分组内。
		groups := map[string]bool{}
		for _, d := range got {
			groups[d.GroupKey] = true
		}
		if len(groups) != 5 {
			t.Fatalf("通道 %s 期望投递落在 5 个分组，实际 %d 个分组", n.id, len(groups))
		}
		t.Logf("通道 %s 串行投递 %d 条通知，覆盖 %d 条不同告警、%d 个分组，无并发重叠、无重复",
			n.id, len(got), len(seen), len(groups))
	}
}

// TestCloseDrainsInFlight Close 等待在途投递完成，不丢在途告警。
func TestCloseDrainsInFlight(t *testing.T) {
	n := newFakeNotifier("local-1")
	e := NewEngine(Config{GroupBy: []string{"service"}, MaxAttempts: 1},
		[]Notifier{n}, newFakeClock(time.Now()))
	d := e.Submit(firingEvent("fp-1", LabelSet{"service": "api"}))
	if d.Reason != ReasonNotified {
		t.Fatalf("期望 notified，实际 %s", d.Reason)
	}
	e.Close()
	if got := n.deliveries(); len(got) != 1 {
		t.Fatalf("Close 不应丢弃在途投递，期望 1 条，实际 %d", len(got))
	}
	if d2 := e.Submit(firingEvent("fp-2", LabelSet{"service": "api"})); d2.Reason != ReasonClosed {
		t.Fatalf("关闭后事件期望 engine_closed，实际 %s", d2.Reason)
	}
}
