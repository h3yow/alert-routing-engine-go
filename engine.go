package alertrouter

import (
	"sync"
	"sync/atomic"
	"time"
)

// Reason 是一条事件的最终处理结论原因，不同丢弃原因互不相同、可区分。
type Reason string

const (
	// ReasonNotified 触发事件已进入投递。
	ReasonNotified Reason = "notified"
	// ReasonResolved 恢复事件与此前触发对应上，已进入恢复通知投递。
	ReasonResolved Reason = "resolved"
	// ReasonDuplicate 触发中的重复事件，去重丢弃。
	ReasonDuplicate Reason = "duplicate"
	// ReasonSuppressed 命中抑制规则，不通知，状态正常跟踪。
	ReasonSuppressed Reason = "suppressed"
	// ReasonSilenced 命中静默规则，不通知，状态正常跟踪（优先级高于抑制）。
	ReasonSilenced Reason = "silenced"
	// ReasonOrphanResolve 没有对应触发的孤立恢复，不通知，不落状态。
	ReasonOrphanResolve Reason = "orphan_resolve"
	// ReasonOutOfOrder 已恢复后重复到达的恢复（乱序/过期），不通知。
	ReasonOutOfOrder Reason = "out_of_order"
	// ReasonCoalesced 主动丢弃：同分组相同状态的投递已在排队/在途，投递层合并。
	ReasonCoalesced Reason = "delivery_coalesced"
	// ReasonClosed 引擎关闭后到达的事件。
	ReasonClosed Reason = "engine_closed"
	// ReasonDeliveryDropped 异步投递结论：重试耗尽，投递层丢弃（与主动丢弃区分）。
	ReasonDeliveryDropped Reason = "delivery_dropped"
)

// Decision 是 Submit 的同步判定结果。
type Decision struct {
	Fingerprint string
	GroupKey    string
	Reason      Reason
	// 命中的规则名，便于测试打印判定依据。
	RuleName string
}

// Config 是引擎配置。
type Config struct {
	// GroupBy 是参与分组键的标签名（有序）。
	GroupBy []string
	// MaxAttempts 单通道投递最大尝试次数，<=0 时取 3。
	MaxAttempts int
	// BaseBackoff 首次重试退避，<=0 时取 100ms，之后指数翻倍。
	BaseBackoff time.Duration
}

// Engine 是告警路由引擎：状态管理、抑制静默、并发投递全部本地完成。
type Engine struct {
	cfg   Config
	clock Clock

	store *StateStore
	rules atomic.Pointer[RuleSet]
	disp  *Dispatcher

	resMu   sync.Mutex
	results []DeliveryResult
	closed  atomic.Bool
}

// NewEngine 创建引擎并启动投递层。
func NewEngine(cfg Config, notifiers []Notifier, clock Clock) *Engine {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	e := &Engine{
		cfg:   cfg,
		clock: clock,
		store: newStateStore(),
	}
	e.rules.Store(&RuleSet{})
	e.disp = newDispatcher(notifiers, clock, cfg.MaxAttempts, cfg.BaseBackoff, e.recordResult)
	return e
}

// Submit 接收一条告警事件：先同步推进状态机，再做规则判定，最后（如需要）进入投递。
// 状态流转在投递开始前就已落定，投递成败与重试不影响告警状态。
// 规则集在进入时取一次快照，因此一次 Submit 内结论确定，不与 ReloadRules 互相干扰。
func (e *Engine) Submit(ev AlertEvent) Decision {
	groupKey := ev.GroupKey(e.cfg.GroupBy)
	dec := Decision{Fingerprint: ev.Fingerprint, GroupKey: groupKey}
	if e.closed.Load() {
		dec.Reason = ReasonClosed
		return dec
	}

	now := e.clock.Now()
	outcome := e.store.ingest(ev, now)

	switch outcome {
	case outcomeOrphanResolve:
		dec.Reason = ReasonOrphanResolve
		return dec
	case outcomeStaleResolve:
		dec.Reason = ReasonOutOfOrder
		return dec
	case outcomeDuplicateFiring:
		dec.Reason = ReasonDuplicate
		return dec
	}

	// newFiring / resolved 都需要经过静默/抑制判定。
	rs := e.rules.Load()
	verdict := rs.evaluate(ev, e.store.snapshotFiring(), now)
	if verdict.silenced {
		dec.Reason = ReasonSilenced
		dec.RuleName = verdict.silenceRule
		return dec
	}
	if verdict.inhibited {
		dec.Reason = ReasonSuppressed
		dec.RuleName = verdict.inhibitRule
		return dec
	}

	n := Notification{GroupKey: groupKey, Status: ev.Status, Alerts: []AlertEvent{ev}}
	switch e.disp.submit(n) {
	case submitCoalesced:
		dec.Reason = ReasonCoalesced
	case submitClosed:
		dec.Reason = ReasonClosed
	default:
		if ev.Status == EventResolved {
			dec.Reason = ReasonResolved
		} else {
			dec.Reason = ReasonNotified
		}
	}
	return dec
}

// ReloadRules 在运行中整体替换规则集。
// 兼容规则：
//   - 快照替换，进行中的 Submit 仍使用进入时取到的旧快照，结论不变；
//   - 已在状态机中的告警不丢弃，已排队/在途的投递不取消、不补发，不会因重载产生重复通知；
//   - 规则只对重载之后的新判定生效。
func (e *Engine) ReloadRules(rs RuleSet) {
	cp := rs
	e.rules.Store(&cp)
}

func (e *Engine) recordResult(r DeliveryResult) {
	e.resMu.Lock()
	e.results = append(e.results, r)
	e.resMu.Unlock()
}

// DrainResults 返回并清空目前已完成的异步投递结果。
// 投递失败（Delivered=false, Reason=ReasonDeliveryDropped 可由调用方据 Err 判定）
// 与同步的主动合并丢弃 ReasonCoalesced 通过不同渠道区分。
func (e *Engine) DrainResults() []DeliveryResult {
	e.resMu.Lock()
	defer e.resMu.Unlock()
	out := e.results
	e.results = nil
	return out
}

// waitSettled 等待所有已入队通知走完投递生命周期（含退避重试）并到达终态。
func (e *Engine) waitSettled() { e.disp.waitSettled() }

// Close 停止接收新事件并等待所有在途投递结束，不丢在途告警。
func (e *Engine) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	e.disp.close()
}
