// Command demo runs the alert routing engine entirely in-process: alerts
// go in, grouped/deduped/silenced/inhibited decisions come out and are
// delivered to in-memory channels. No external service is contacted.
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
	"github.com/h3yow/alert-routing-engine-go/clock"
	"github.com/h3yow/alert-routing-engine-go/engine"
	"github.com/h3yow/alert-routing-engine-go/notify"
	"github.com/h3yow/alert-routing-engine-go/rule"
)

type memChannel struct {
	name string
	fail int32 // number of sends that fail before succeeding
}

func (c *memChannel) Name() string { return c.name }

func (c *memChannel) Send(ctx context.Context, n notify.Notification) error {
	if atomic.AddInt32(&c.fail, -1) >= 0 {
		return fmt.Errorf("channel %s simulated failure", c.name)
	}
	fmt.Printf("[channel=%s] deliver round=%d kind=%s fp=%s group=%q labels=%v\n",
		c.name, n.Round, n.Kind, n.Fingerprint, n.Group, n.Labels)
	return nil
}

func mkAlert(fp string, kind alert.EventKind, labels map[string]string, t time.Time) alert.Alert {
	a := alert.Alert{Fingerprint: fp, Labels: labels, Kind: kind}
	if kind == alert.KindFiring {
		a.StartsAt = t
	} else {
		a.EndsAt = t
	}
	return a
}

func main() {
	clk := clock.NewRealClock()
	now := clk.Now()

	channels := []notify.Channel{
		&memChannel{name: "ops-board"},
		&memChannel{name: "oncall-phone", fail: 2}, // exercises retry+backoff
	}

	// Silence host=node-3 for a wide window; inhibit warnings on a host
	// while a critical alert for that host is active.
	rules := rule.NewMatchers(
		[]rule.SilenceRule{{
			ID:       "silence-node3",
			Equal:    map[string]string{"host": "node-3"},
			StartsAt: now.Add(-time.Hour),
			EndsAt:   now.Add(time.Hour),
		}},
		[]rule.InhibitRule{{
			ID:          "inhibit-warning-by-critical",
			SourceEqual: map[string]string{"severity": "critical"},
			TargetEqual: map[string]string{"severity": "warning"},
			Equal:       []string{"host"},
		}},
	)

	rec := engine.NewRecorder()
	eng := engine.New(clk, channels, rules, engine.Config{GroupBy: []string{"alertname", "host"}}, rec)
	defer eng.Close()

	ctx := context.Background()
	cpuLabels := map[string]string{"alertname": "CPUHigh", "host": "node-1", "severity": "warning"}
	events := []alert.Alert{
		mkAlert("cpu-1", alert.KindFiring, cpuLabels, now), // round 1 fires
		mkAlert("cpu-1", alert.KindFiring, cpuLabels, now), // duplicate within round 1
		mkAlert("cpu-2", alert.KindFiring, map[string]string{"alertname": "NodeDown", "host": "node-1", "severity": "critical"}, now),
		mkAlert("cpu-3", alert.KindFiring, cpuLabels, now), // inhibited by cpu-2
		mkAlert("cpu-4", alert.KindFiring, map[string]string{"alertname": "DiskFull", "host": "node-3", "severity": "critical"}, now),   // silenced
		mkAlert("ghost", alert.KindResolved, map[string]string{"alertname": "Mystery", "host": "node-9"}, now),                          // orphan
		mkAlert("cpu-1", alert.KindResolved, cpuLabels, now),                                                                            // round 1 resolves
		mkAlert("cpu-2", alert.KindResolved, map[string]string{"alertname": "NodeDown", "host": "node-1", "severity": "critical"}, now), // inhibition source clears
		mkAlert("cpu-1", alert.KindFiring, cpuLabels, now),                                                                              // flapping: round 2 opens, rules now clear -> notifies again
		mkAlert("cpu-1", alert.KindResolved, cpuLabels, now),                                                                            // round 2 resolves -> 2nd resolution
		mkAlert("cpu-1", alert.KindResolved, cpuLabels, now),                                                                            // duplicate recovery (no new round)
		mkAlert("ghost", alert.KindFiring, map[string]string{"alertname": "Mystery", "host": "node-9"}, now),                            // out of order after orphan
	}

	for _, a := range events {
		d := eng.Ingest(ctx, a)
		fmt.Printf("ingest round=%d kind=%-8s fp=%-6s -> outcome=%s reason=%s\n",
			d.Round, a.Kind, a.Fingerprint, d.Outcome, d.Reason)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := eng.WaitIdle(waitCtx); err != nil {
		fmt.Println("wait idle:", err)
	}

	fmt.Println("--- delivery results ---")
	for _, r := range rec.Results() {
		switch {
		case r.Delivered:
			fmt.Printf("DELIVERED channel=%s round=%d id=%s attempts=%d\n", r.Channel, r.Notification.Round, r.Notification.ID, r.Attempts)
		default:
			fmt.Printf("DROPPED   channel=%s round=%d id=%s reason=%s attempts=%d err=%v\n",
				r.Channel, r.Notification.Round, r.Notification.ID, r.DropReason, r.Attempts, r.Err)
		}
	}
}
