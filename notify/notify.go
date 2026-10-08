// Package notify defines local notification channels and delivery tasks.
package notify

import (
	"context"
	"time"

	"github.com/h3yow/alert-routing-engine-go/alert"
)

// Notification is one fan-out item derived from an engine decision.
type Notification struct {
	ID          string
	Fingerprint string
	Group       string
	Kind        alert.EventKind
	Labels      map[string]string
	At          time.Time
	// Round identifies the fault episode this notification belongs to.
	// Every round delivers one firing and (when applicable) one resolution
	// notification per channel; rounds never replace each other's messages.
	Round int
	// RuleID records the silence/inhibition rule involved ("" when firing
	// normally). Only meaningful for policy-recorded notifications.
	RuleID string
}

// Channel is a local notification sink. Send returns nil on success.
// Implementations must be safe for concurrent use.
type Channel interface {
	Name() string
	Send(ctx context.Context, n Notification) error
}

// DropReason distinguishes failure classes recorded on the trail.
type DropReason int

const (
	// DropExhausted: all retry attempts failed.
	DropExhausted DropReason = iota
	// DropChannelGone: the channel was removed on reload / engine stopped.
	DropChannelGone
	// DropStale: the alert was resolved/silenced before delivery completed.
	DropStale
)

func (r DropReason) String() string { return dropReasonNames[r] }

var dropReasonNames = [...]string{"retry_exhausted", "channel_gone", "stale"}

// Result is the dispatcher's final conclusion for one notification.
type Result struct {
	Notification Notification
	Channel      string
	Delivered    bool
	Attempts     int
	DropReason   DropReason
	Err          error
	At           time.Time
}
