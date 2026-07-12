package transport

import (
	"errors"

	clusterprotocol "github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
)

const ProtocolVersionV1 = clusterprotocol.Version1

const (
	TypeHello     = clusterprotocol.MessageHello
	TypeWelcome   = clusterprotocol.MessageWelcome
	TypeHeartbeat = clusterprotocol.MessageHeartbeat
	TypeAck       = clusterprotocol.MessageAck
	TypeNack      = clusterprotocol.MessageNack
)

var (
	ErrNotConnected      = errors.New("cluster transport is not connected")
	ErrSessionClosed     = errors.New("cluster transport session is closed")
	ErrSessionSuperseded = errors.New("cluster transport session was superseded")
)

// These aliases keep transport-facing APIs concise while the protocol package
// remains the single source of truth for every wire type.
type Envelope = clusterprotocol.Envelope
type Hello = clusterprotocol.Hello
type Welcome = clusterprotocol.Welcome
type Heartbeat = clusterprotocol.Heartbeat
type Ack = clusterprotocol.Ack
type Nack = clusterprotocol.Nack

func NewEnvelope(messageType clusterprotocol.MessageType, payload any) (Envelope, error) {
	message, err := clusterprotocol.NewEnvelope(messageType, payload)
	if err != nil {
		return Envelope{}, err
	}
	return *message, nil
}

func validateTransportEnvelope(message Envelope) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if message.Seq == 0 {
		return errors.New("seq must be greater than zero")
	}
	return nil
}

func decodePayload[T any](message Envelope) (T, error) {
	return clusterprotocol.DecodePayload[T](message)
}
