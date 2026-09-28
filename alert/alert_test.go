package alert

import "testing"

func TestGroupKey(t *testing.T) {
	ls := map[string]string{"alertname": "CPUHigh", "host": "n1", "severity": "warn"}

	// No grouping labels => single global group.
	if got := GroupKey(ls, nil); got != "" {
		t.Fatalf("empty groupBy => %q, want %q", got, "")
	}

	// Order of groupBy names must not change the key.
	k1 := GroupKey(ls, []string{"alertname", "host"})
	k2 := GroupKey(ls, []string{"host", "alertname"})
	if k1 != k2 {
		t.Fatalf("group key order-sensitive: %q vs %q", k1, k2)
	}

	// Different grouping label values land in different groups.
	other := map[string]string{"alertname": "CPUHigh", "host": "n2", "severity": "warn"}
	if GroupKey(ls, []string{"alertname", "host"}) == GroupKey(other, []string{"alertname", "host"}) {
		t.Fatal("alerts differing on a grouping label share a group")
	}
	// Labels outside groupBy must not affect grouping.
	other2 := map[string]string{"alertname": "CPUHigh", "host": "n1", "severity": "critical"}
	if GroupKey(ls, []string{"alertname", "host"}) != GroupKey(other2, []string{"alertname", "host"}) {
		t.Fatal("non-grouping label changed the group key")
	}
}

func TestKindAndStatusStrings(t *testing.T) {
	if KindFiring.String() != "firing" || KindResolved.String() != "resolved" {
		t.Fatal("kind strings broken")
	}
	for o, want := range map[Outcome]string{
		OutcomeNotified:               "notified",
		OutcomeSuppressed:             "suppressed",
		OutcomeSilenced:               "silenced",
		OutcomeDuplicate:              "duplicate",
		OutcomeOrphanResolve:          "orphan_resolve",
		OutcomeOutOfOrder:             "out_of_order",
		OutcomeResolvedNotification:   "resolved_notification",
		OutcomeResolvedNoNotification: "resolved_no_notification",
	} {
		if o.String() != want {
			t.Errorf("outcome %d => %q, want %q", o, o.String(), want)
		}
	}
}
