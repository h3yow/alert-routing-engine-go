package alertrouter

import (
	"context"
	"sync"
	"time"
)

// Notifier 是本地通知通道的抽象，测试中用内存假通道实现，不接任何真实外部服务。
type Notifier interface {
	ID() string
	Notify(ctx context.Context, n Notification) error
}

// Notification 是一次投递的内容，按分组聚合。
type Notification struct {
	GroupKey string
	Status   EventType
	Alerts   []AlertEvent
}

func sameNotification(a, b Notification) bool {
	return a.GroupKey == b.GroupKey && a.Status == b.Status
}

// DeliveryResult 是单个通知在单个通道上的最终投递结果。
type DeliveryResult struct {
	GroupKey   string
	Status     EventType
	NotifierID string
	Attempts   int
	// Delivered 为 true 表示成功；false 表示重试耗尽被投递层丢弃。
	Delivered bool
	Err       error
}

// submitResult 是投递入队的同步结论。
type submitResult int

const (
	submitQueued    submitResult = iota // 已排队投递
	submitCoalesced                     // 主动丢弃：同分组已有相同的在途/排队投递，合并
	submitClosed                        // 引擎关闭中
)

// groupGate 保证同一分组的通知严格串行。queued 是唯一事实源，ch 仅作唤醒信号。
// 同组、同状态、同指纹的并发通知被聚合去重；不同告警会聚合进同一批排队通知，
// 在途期间到达的新告警挂起为下一批，任何告警都不会被静默丢弃。
type groupGate struct {
	ch     chan struct{}
	queued []Notification
	active bool
}

// Dispatcher 负责把通知并发投递到多个本地通道：
// 不同分组 / 不同通道之间并发；同一分组串行；
// 失败按指数退避重试，重试耗尽才算投递失败，与"主动丢弃/合并"明确区分。
type Dispatcher struct {
	notifiers   []Notifier
	clock       Clock
	maxAttempts int
	baseBackoff time.Duration

	wg sync.WaitGroup

	mu       sync.Mutex
	gates    map[string]*groupGate
	closing  bool
	queued   int           // 已入队尚未被 worker 取走
	inflight int           // 投递生命周期计数（含退避等待，按 通知×通道）
	sem      chan struct{} // 计数器每次变化都发信号，容量缓冲避免唤醒丢失
	onResult func(DeliveryResult)
}

func newDispatcher(notifiers []Notifier, clock Clock, maxAttempts int, baseBackoff time.Duration, onResult func(DeliveryResult)) *Dispatcher {
	d := &Dispatcher{
		notifiers:   notifiers,
		clock:       clock,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
		gates:       map[string]*groupGate{},
		onResult:    onResult,
		sem:         make(chan struct{}, 1024),
	}
	return d
}

// signal 非阻塞通知一次计数变化（sem 有缓冲，不会因等待者尚未就绪而丢信号）。
// 调用方必须持有 d.mu。
func (d *Dispatcher) signal() {
	select {
	case d.sem <- struct{}{}:
	default:
	}
}

// waitFor 在锁内反复检查 pred，pred 为假时释放锁等待一次新信号再复查。
// 每次等待前在持锁状态排空陈旧信号（计数器持锁期间不会再变化），
// 因此醒来后收到的必然是下一次真实变化，不会被历史信号卡住或错过新信号。
func (d *Dispatcher) waitFor(pred func(queued, inflight int) bool) {
	d.mu.Lock()
	for !pred(d.queued, d.inflight) {
		// 排空历史信号。
		for {
			select {
			case <-d.sem:
				continue
			default:
			}
			break
		}
		d.mu.Unlock()
		<-d.sem
		d.mu.Lock()
	}
	d.mu.Unlock()
}

// submit 把通知交给投递层：
//   - 同组同状态已有排队通知：把新告警并入（指纹去重），返回 submitCoalesced（聚合，非报错）；
//   - 同组同状态正在投递：挂起为下一批（不丢失，稍后串行投递），返回 submitCoalesced；
//   - 其余情况入新批，返回 submitQueued。
//
// 同分组永不并发投递；submitCoalesced 是主动聚合决策，与"重试耗尽的投递失败"明确区分。
func (d *Dispatcher) submit(n Notification) submitResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return submitClosed
	}
	g, ok := d.gates[n.GroupKey]
	if !ok {
		g = &groupGate{ch: make(chan struct{}, 1)}
		d.gates[n.GroupKey] = g
		d.wg.Add(1)
		go d.runGroup(n.GroupKey, g)
	}

	// 已排队的同状态通知：聚合告警，指纹去重。
	for i := range g.queued {
		if sameNotification(g.queued[i], n) {
			mergeAlert(&g.queued[i], n.Alerts[0])
			return submitCoalesced
		}
	}

	needWake := len(g.queued) == 0 && !g.active
	g.queued = append(g.queued, n)
	d.queued++
	if needWake {
		// worker 正阻塞等待，唤醒它取走这批；容量 1 且此刻必为空。
		g.ch <- struct{}{}
	}
	d.signal()
	return submitQueued
}

// mergeAlert 把告警 a 并入 n（指纹相同则不重复加入）。
func mergeAlert(n *Notification, a AlertEvent) {
	for _, ex := range n.Alerts {
		if ex.Fingerprint == a.Fingerprint {
			return
		}
	}
	n.Alerts = append(n.Alerts, a)
}

func (d *Dispatcher) runGroup(key string, g *groupGate) {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		if len(g.queued) == 0 {
			if d.closing {
				d.mu.Unlock()
				return
			}
			d.mu.Unlock()
			<-g.ch // 等待新批或 close
			continue
		}
		n := g.queued[0]
		g.queued = g.queued[1:]
		d.queued--
		g.active = true
		// 按通道数预占计数：goroutine 尚未被调度时，等待者也不会误判为结束。
		d.inflight += len(d.notifiers)
		d.mu.Unlock()

		d.fanOut(n)

		d.mu.Lock()
		g.active = false
		d.signal()
		d.mu.Unlock()
	}
}

// fanOut 并发投递到所有本地通道，每个通道独立重试，互不影响。
func (d *Dispatcher) fanOut(n Notification) {
	var wg sync.WaitGroup
	for _, ntf := range d.notifiers {
		wg.Add(1)
		go func(ntf Notifier) {
			defer wg.Done()
			attempts, err := d.deliverWithRetry(ntf, n)
			// 终态：归还 runGroup 预占的一个通道计数。
			d.mu.Lock()
			d.inflight--
			d.signal()
			d.mu.Unlock()
			if d.onResult != nil {
				d.onResult(DeliveryResult{
					GroupKey:   n.GroupKey,
					Status:     n.Status,
					NotifierID: ntf.ID(),
					Attempts:   attempts,
					Delivered:  err == nil,
					Err:        err,
				})
			}
		}(ntf)
	}
	wg.Wait()
}

// deliverWithRetry 指数退避重试：backoff = base * 2^(attempt-1)。
// 退避等待通过注入时钟完成，测试中可确定性推进，无需 sleep。
func (d *Dispatcher) deliverWithRetry(ntf Notifier, n Notification) (int, error) {
	ctx := context.Background()
	attempt := 0
	for {
		attempt++
		err := ntf.Notify(ctx, n)
		if err == nil {
			return attempt, nil
		}
		if attempt >= d.maxAttempts {
			return attempt, err
		}
		backoff := d.baseBackoff << (attempt - 1)
		// 退避等待通过注入时钟完成；inflight 覆盖整个生命周期（含退避），
		// 因此重试期间等待者不会误判为"全部结束"，重试也不触碰告警状态。
		<-d.clock.After(backoff)
	}
}

// waitSettled 等待所有已入队通知走完整个投递生命周期
// （含各通道退避与重试）并到达终态，用于确定性地等待投递全部结束。
func (d *Dispatcher) waitSettled() {
	d.waitFor(func(q, i int) bool { return q == 0 && i == 0 })
}

// close 停止接收新通知，等待在途投递（含重试）全部结束。
func (d *Dispatcher) close() {
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return
	}
	d.closing = true
	gates := d.gates
	d.gates = map[string]*groupGate{}
	d.mu.Unlock()
	for _, g := range gates {
		// 关闭信号通道唤醒可能在等待的 worker；已排队的通知仍会被 worker 取完。
		close(g.ch)
	}
	d.wg.Wait()
}
