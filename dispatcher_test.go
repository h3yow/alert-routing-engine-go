package alertrouter

import "testing"

// TestMergeAlert 聚合时相同指纹不重复并入，不同指纹保留，确保聚合不丢告、不重复。
func TestMergeAlert(t *testing.T) {
	n := Notification{GroupKey: "g", Status: EventFiring, Alerts: []AlertEvent{
		{Fingerprint: "a"}, {Fingerprint: "b"},
	}}
	mergeAlert(&n, AlertEvent{Fingerprint: "a"}) // 重复指纹
	if len(n.Alerts) != 2 {
		t.Fatalf("相同指纹不应重复并入，实际 %d 条", len(n.Alerts))
	}
	mergeAlert(&n, AlertEvent{Fingerprint: "c"}) // 新指纹
	if len(n.Alerts) != 3 {
		t.Fatalf("不同指纹应并入同一批，实际 %d 条", len(n.Alerts))
	}
	t.Logf("聚合结果指纹: a,b,c => %v (判定依据：指纹相同去重，不同聚合)", n.Alerts)
}

func TestSameNotification(t *testing.T) {
	n1 := Notification{GroupKey: "g", Status: EventFiring}
	n2 := Notification{GroupKey: "g", Status: EventFiring}
	n3 := Notification{GroupKey: "g", Status: EventResolved}
	if !sameNotification(n1, n2) || sameNotification(n1, n3) {
		t.Fatal("相同判定只应基于分组键+状态")
	}
}
