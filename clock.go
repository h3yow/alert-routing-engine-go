package alertrouter

import (
	"sort"
	"sync"
	"time"
)

// Clock 抽象时间，生产用真实时钟，测试用可控假时钟，保证时间行为可确定复现。
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// realClock 生产环境使用的真实时钟。
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock 返回生产环境使用的真实时钟。
func RealClock() Clock { return realClock{} }

// fakeTimer 是 fakeClock 上的一个待触发定时器。
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// fakeClock 测试用可控时钟：时间只在调用 Add 时前进，
// 所有通过 After 注册的定时器在到期点由 Add 同步触发，
// 因此时间相关行为可完全确定地复现，测试无需 sleep 等待。
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	// sem 在"登记定时器 / 触发并移除定时器"时发信号，供 waitPending 做确定性屏障。
	sem chan struct{}
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, sem: make(chan struct{}, 1024)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// signal 非阻塞发一次信号，调用方持有 c.mu。
func (c *fakeClock) signal() {
	select {
	case c.sem <- struct{}{}:
	default:
	}
}

// After 返回一个容量为 1 的定时器通道。
// 若登记时截止时刻已到/已过（例如时钟已被推进后才登记），立即触发，
// 保证退避一旦到期就不会丢失。
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		deadline: c.now.Add(d),
		ch:       make(chan time.Time, 1),
	}
	if !t.deadline.After(c.now) {
		// 截止时刻已到或已过：立即触发，不进入待触发列表。
		t.ch <- c.now
		return t.ch
	}
	c.timers = append(c.timers, t)
	c.signal()
	return t.ch
}

// pending 返回已登记但尚未触发的定时器数量。
func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// waitPending 阻塞直到待触发定时器数量恰好为 n。
// 这是测试推进时间前的精确屏障：确认所有该登记的退避定时器都已同步登记。
// 每次等待前排空历史信号，因此不会被陈旧信号提前放行。
func (c *fakeClock) waitPending(n int) {
	c.mu.Lock()
	for len(c.timers) != n {
		for {
			select {
			case <-c.sem:
				continue
			default:
			}
			break
		}
		c.mu.Unlock()
		<-c.sem
		c.mu.Lock()
	}
	c.mu.Unlock()
}

// Add 推进时钟 d，并同步触发所有已到期的定时器（按到期顺序）。
func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	rest := c.timers[:0]
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			due = append(due, t)
		} else {
			rest = append(rest, t)
		}
	}
	c.timers = rest
	if len(due) > 0 {
		c.signal()
	}
	c.mu.Unlock()

	sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		// 容量为 1 且定时器只会触发一次，非阻塞写入即可。
		t.ch <- t.deadline
	}
}
