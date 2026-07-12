package transport

import "context"

// Peer describes the authenticated remote endpoint visible to message
// handlers. Implementations intentionally do not expose the WebSocket itself.
type Peer interface {
	NodeID() string
	SessionID() string
	ConnectionEpoch() uint64
	Send(context.Context, Envelope) error
}

type SequencePeer interface {
	Peer
	LastReceivedSeq() uint64
	LastSentSeq() uint64
}

// Handler is called serially for each received message. Returning nil means
// the message has been accepted by the upper layer and allows the transport to
// emit an ACK. Returning an error emits a NACK and keeps the connection alive.
type Handler interface {
	HandleMessage(context.Context, Peer, Envelope) error
}

type HandlerFunc func(context.Context, Peer, Envelope) error

func (f HandlerFunc) HandleMessage(ctx context.Context, peer Peer, message Envelope) error {
	return f(ctx, peer, message)
}

type nopHandler struct{}

func (nopHandler) HandleMessage(context.Context, Peer, Envelope) error { return nil }
