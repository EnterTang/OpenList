package model

import "time"

const (
	SubscriptionTelegramEventStatusPending    = "pending"
	SubscriptionTelegramEventStatusProcessing = "processing"
	SubscriptionTelegramEventStatusProcessed  = "processed"
	SubscriptionTelegramEventStatusRetryWait  = "retry_wait"
	SubscriptionTelegramEventStatusDeadLetter = "dead_letter"

	SubscriptionRealtimeCandidateStatusPending  = "pending"
	SubscriptionRealtimeCandidateStatusSelected = "selected"
	SubscriptionRealtimeCandidateStatusSkipped  = "skipped"
	SubscriptionRealtimeCandidateStatusFailed   = "failed"
)

// SubscriptionTelegramEvent is the Coordinator's durable inbox for a message
// received from a realtime Telegram session. The unique tuple prevents replayed
// updates, reconnect catch-up, and the polling scheduler from creating a second
// realtime observation for the same subscription message.
type SubscriptionTelegramEvent struct {
	ID             uint       `json:"id" gorm:"primarykey;index:idx_subscription_telegram_events_latest,priority:3"`
	CreatedAt      time.Time  `json:"created_at" gorm:"index:idx_subscription_telegram_events_latest,priority:2"`
	UpdatedAt      time.Time  `json:"updated_at"`
	SubscriptionID uint       `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_telegram_event_message;index;index:idx_subscription_telegram_events_latest,priority:1"`
	Channel        string     `json:"channel" gorm:"uniqueIndex:idx_subscription_telegram_event_message;size:191"`
	MessageID      string     `json:"message_id" gorm:"uniqueIndex:idx_subscription_telegram_event_message;size:64"`
	PayloadJSON    string     `json:"-" gorm:"type:text"`
	PayloadHash    string     `json:"payload_hash" gorm:"size:64"`
	Status         string     `json:"status" gorm:"index;size:32"`
	Attempts       int        `json:"attempts"`
	AvailableAt    time.Time  `json:"available_at" gorm:"index"`
	ProcessedAt    *time.Time `json:"processed_at,omitempty"`
	LastError      string     `json:"last_error,omitempty" gorm:"type:text"`
	ObservationKey string     `json:"observation_key,omitempty" gorm:"size:64"`
}

// SubscriptionRealtimeCandidate captures an inspected media option while the
// Coordinator waits briefly for a preferred provider to publish the same slot.
// It is deliberately independent of a Worker so only the final media task is
// assigned through normal cluster scheduling.
type SubscriptionRealtimeCandidate struct {
	ID             uint      `json:"id" gorm:"primarykey"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	SubscriptionID uint      `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_realtime_candidate;index"`
	SlotKey        string    `json:"slot_key" gorm:"uniqueIndex:idx_subscription_realtime_candidate;index;size:191"`
	SourceKey      string    `json:"source_key" gorm:"uniqueIndex:idx_subscription_realtime_candidate;size:191"`
	FileHash       string    `json:"file_hash" gorm:"uniqueIndex:idx_subscription_realtime_candidate;size:128"`
	ItemID         uint      `json:"item_id" gorm:"index"`
	Provider       string    `json:"provider" gorm:"index;size:32"`
	ShareURL       string    `json:"share_url" gorm:"type:text"`
	SharePasscode  string    `json:"-" gorm:"size:64"`
	MessageID      string    `json:"message_id" gorm:"size:64"`
	MessageChannel string    `json:"message_channel" gorm:"size:191"`
	MessageURL     string    `json:"message_url" gorm:"type:text"`
	MessageText    string    `json:"message_text" gorm:"type:text"`
	ReadyAt        time.Time `json:"ready_at" gorm:"index"`
	Status         string    `json:"status" gorm:"index;size:32"`
	LastError      string    `json:"last_error,omitempty" gorm:"type:text"`
}
