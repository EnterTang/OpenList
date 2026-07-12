package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const Version1 = "1.0"

type MessageType string

const (
	MessageHello                MessageType = "hello"
	MessageWelcome              MessageType = "welcome"
	MessageHeartbeat            MessageType = "heartbeat"
	MessageInventoryReport      MessageType = "inventory.report"
	MessageInventoryQuery       MessageType = "inventory.query"
	MessageConfigApply          MessageType = "config.apply"
	MessageConfigObserved       MessageType = "config.observed"
	MessageStorageApply         MessageType = "storage.apply"
	MessageStorageApplyResult   MessageType = "storage.apply_result"
	MessageJobOffer             MessageType = "job.offer"
	MessageJobAccept            MessageType = "job.accept"
	MessageJobReject            MessageType = "job.reject"
	MessageJobProgress          MessageType = "job.progress"
	MessageJobCheckpoint        MessageType = "job.checkpoint"
	MessageJobResult            MessageType = "job.result"
	MessageJobCancel            MessageType = "job.cancel"
	MessageLeaseRenew           MessageType = "lease.renew"
	MessageStagePermitRequest   MessageType = "stage.permit_request"
	MessageStagePermit          MessageType = "stage.permit"
	MessageUploadETFManifest    MessageType = "upload.etf_manifest"
	MessageUploadETFManifestAck MessageType = "upload.etf_manifest_ack"
	MessageResultQueueStats     MessageType = "result.queue.stats"
	MessageAck                  MessageType = "ack"
	MessageNack                 MessageType = "nack"
)

// Envelope is the stable Cluster Protocol v1 wire wrapper. Payload stays raw
// until the message type is authenticated and selected by the receiver.
type Envelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	Type            MessageType     `json:"type"`
	MessageID       string          `json:"message_id"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	NodeID          string          `json:"node_id,omitempty"`
	SessionID       string          `json:"session_id,omitempty"`
	Seq             uint64          `json:"seq"`
	SentAt          time.Time       `json:"sent_at"`
	Payload         json.RawMessage `json:"payload"`
}

func NewEnvelope(messageType MessageType, payload any) (*Envelope, error) {
	if messageType == "" {
		return nil, errors.New("message type is required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal cluster payload: %w", err)
	}
	return &Envelope{
		ProtocolVersion: Version1,
		Type:            messageType,
		MessageID:       uuid.NewString(),
		SentAt:          time.Now().UTC(),
		Payload:         raw,
	}, nil
}

func (e Envelope) Validate() error {
	if e.ProtocolVersion != Version1 {
		return fmt.Errorf("unsupported protocol_version %q", e.ProtocolVersion)
	}
	if e.Type == "" {
		return errors.New("message type is required")
	}
	if e.MessageID == "" {
		return errors.New("message_id is required")
	}
	if e.Seq == 0 {
		return errors.New("seq must be greater than zero")
	}
	if e.SentAt.IsZero() {
		return errors.New("sent_at is required")
	}
	if len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return errors.New("payload must be valid json")
	}
	return nil
}

func (e Envelope) DecodePayload(dst any) error {
	if err := json.Unmarshal(e.Payload, dst); err != nil {
		return fmt.Errorf("decode %s payload: %w", e.Type, err)
	}
	return nil
}

func DecodePayload[T any](e Envelope) (T, error) {
	var payload T
	if err := e.DecodePayload(&payload); err != nil {
		return payload, err
	}
	return payload, nil
}
