package engine

import (
	"sync"

	"github.com/h3yow/alert-routing-engine-go/alert"
	"github.com/h3yow/alert-routing-engine-go/notify"
)

// Decision records the engine's conclusion for one input event: the
// outcome class, the rule (if any) behind it, a human-readable reason
// and the notifications produced. Distinct outcome values keep repeated,
// out-of-order and orphan-recovery cases from swallowing one another.
type Decision struct {
	Alert    alert.Alert
	Outcome  alert.Outcome
	RuleID   string
	Reason   string
	Messages []notify.Notification
}

// Recorder collects decisions and terminal delivery results. It is the
// assertion surface for tests and the input for structured logs.
type Recorder struct {
	mu        sync.Mutex
	decisions []Decision
	results   []notify.Result
}

// NewRecorder constructs an empty recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// RecordDecision appends one decision.
func (r *Recorder) RecordDecision(d Decision) {
	r.mu.Lock()
	r.decisions = append(r.decisions, d)
	r.mu.Unlock()
}

// RecordResult appends one terminal delivery result.
func (r *Recorder) RecordResult(res notify.Result) {
	r.mu.Lock()
	r.results = append(r.results, res)
	r.mu.Unlock()
}

// Decisions returns a copy of all recorded decisions.
func (r *Recorder) Decisions() []Decision {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Decision, len(r.decisions))
	copy(out, r.decisions)
	return out
}

// Results returns a copy of all recorded delivery results.
func (r *Recorder) Results() []notify.Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]notify.Result, len(r.results))
	copy(out, r.results)
	return out
}
