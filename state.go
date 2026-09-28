package alertrouter

import (
	"sync"
	"time"
)

// phase 是单条告警在引擎内的状态阶段。
type phase int

const (
	phaseFiring phase = iota
	phaseResolved
)

// ingestOutcome 是事件进入状态机后的判定结论。
// 五种结论互斥，分别对应不同处理，不会互相吞掉。
type ingestOutcome int

const (
	// outcomeNewFiring 首次出现的触发，或已恢复后再次触发（新一轮事故）。
	outcomeNewFiring ingestOutcome = iota
	// outcomeDuplicateFiring 已处于触发中时又到达的重复触发。
	outcomeDuplicateFiring
	// outcomeResolved 触发 -> 恢复 的正常流转。
	outcomeResolved
	// outcomeOrphanResolve 此前从未见过触发，孤立的恢复事件。
	outcomeOrphanResolve
	// outcomeStaleResolve 已经恢复过又来的恢复（乱序/过期重复），与孤立恢复区分。
	outcomeStaleResolve
)

type trackedAlert struct {
	event     AlertEvent
	phase     phase
	updatedAt time.Time
}

// StateStore 跟踪所有已知告警的状态，并发安全。
type StateStore struct {
	mu     sync.Mutex
	alerts map[string]*trackedAlert
}

func newStateStore() *StateStore {
	return &StateStore{alerts: map[string]*trackedAlert{}}
}

// ingest 接收一条事件并推进状态机，返回判定结论。
// 状态流转：
//
//	(不存在) + Firing   -> firing，新触发
//	(不存在) + Resolved -> 不落库，孤立恢复
//	firing    + Firing  -> firing，重复触发（去重，不通知）
//	firing    + Resolved-> resolved，正常恢复
//	resolved  + Resolved-> 保持 resolved，过期重复恢复（乱序，不通知）
//	resolved  + Firing  -> firing，再次触发（新一轮，通知）
func (s *StateStore) ingest(ev AlertEvent, now time.Time) ingestOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.alerts[ev.Fingerprint]
	if !ok {
		if ev.Status == EventResolved {
			// 孤立恢复：不创建状态，后续若真有触发到达仍按新触发处理。
			return outcomeOrphanResolve
		}
		s.alerts[ev.Fingerprint] = &trackedAlert{event: ev, phase: phaseFiring, updatedAt: now}
		return outcomeNewFiring
	}
	cur.updatedAt = now
	switch {
	case cur.phase == phaseFiring && ev.Status == EventFiring:
		return outcomeDuplicateFiring
	case cur.phase == phaseFiring && ev.Status == EventResolved:
		cur.phase = phaseResolved
		// 以恢复事件携带的结束时间/标签为准补齐。
		cur.event = ev
		return outcomeResolved
	case cur.phase == phaseResolved && ev.Status == EventResolved:
		return outcomeStaleResolve
	default: // resolved + Firing：新一轮触发
		cur.phase = phaseFiring
		cur.event = ev
		return outcomeNewFiring
	}
}

// snapshotFiring 返回当前处于触发状态的告警副本，供抑制规则判定。
func (s *StateStore) snapshotFiring() []AlertEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AlertEvent, 0, len(s.alerts))
	for _, a := range s.alerts {
		if a.phase == phaseFiring {
			out = append(out, a.event)
		}
	}
	return out
}
