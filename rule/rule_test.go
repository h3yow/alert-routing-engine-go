package rule

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func TestSilenceTimeWindowAndLabels(t *testing.T) {
	m := NewMatchers([]SilenceRule{{
		ID:       "s1",
		Equal:    map[string]string{"host": "n1"},
		StartsAt: t0,
		EndsAt:   t0.Add(time.Hour),
	}}, nil)

	if got := m.MatchSilence(map[string]string{"host": "n1"}, t0.Add(30*time.Minute)); got != "s1" {
		t.Fatalf("active window match = %q, want s1", got)
	}
	// Half-open [start,end): boundaries behave deterministically.
	if got := m.MatchSilence(map[string]string{"host": "n1"}, t0.Add(-time.Nanosecond)); got != "" {
		t.Fatalf("matched before StartsAt: %q", got)
	}
	if got := m.MatchSilence(map[string]string{"host": "n1"}, t0.Add(time.Hour)); got != "" {
		t.Fatalf("matched at/after EndsAt: %q", got)
	}
	// Non-matching label value.
	if got := m.MatchSilence(map[string]string{"host": "n2"}, t0); got != "" {
		t.Fatalf("matched wrong host: %q", got)
	}
}

func TestInhibitRequiresSourceAndEqualLabels(t *testing.T) {
	m := NewMatchers(nil, []InhibitRule{{
		ID:          "i1",
		SourceEqual: map[string]string{"severity": "critical"},
		TargetEqual: map[string]string{"severity": "warning"},
		Equal:       []string{"host"},
	}})

	target := map[string]string{"severity": "warning", "host": "n1"}

	if got := m.MatchInhibit(target, nil); got != "" {
		t.Fatal("inhibited with no sources")
	}
	sourceOtherHost := []map[string]string{{"severity": "critical", "host": "n2"}}
	if got := m.MatchInhibit(target, sourceOtherHost); got != "" {
		t.Fatal("inhibited despite unequal equal-label (host)")
	}
	sourceMatch := []map[string]string{{"severity": "critical", "host": "n1"}}
	if got := m.MatchInhibit(target, sourceMatch); got != "i1" {
		t.Fatalf("matching source => %q, want i1", got)
	}
}

func TestSnapshotIsImmutable(t *testing.T) {
	sil := []SilenceRule{{ID: "s1", Equal: map[string]string{"host": "n1"}, StartsAt: t0, EndsAt: t0.Add(time.Hour)}}
	m := NewMatchers(sil, nil)
	sil[0].ID = "mutated"
	if got := m.MatchSilence(map[string]string{"host": "n1"}, t0); got != "s1" {
		t.Fatalf("snapshot affected by caller mutation: %q", got)
	}
}
