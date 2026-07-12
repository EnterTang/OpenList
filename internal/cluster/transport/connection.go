package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultWriteTimeout      = 10 * time.Second
	defaultHeartbeatInterval = 20 * time.Second
	defaultReadTimeout       = 60 * time.Second
	defaultLeaseDuration     = 60 * time.Second
	defaultMaxMessageBytes   = 1 << 20
	defaultSendQueueSize     = 128
	closeSessionSuperseded   = 4001
)

type connectionOptions struct {
	writeTimeout      time.Duration
	heartbeatInterval time.Duration
	readTimeout       time.Duration
	maxMessageBytes   int64
	sendQueueSize     int
	handler           Handler
}

func (o *connectionOptions) setDefaults() {
	if o.writeTimeout <= 0 {
		o.writeTimeout = defaultWriteTimeout
	}
	if o.heartbeatInterval <= 0 {
		o.heartbeatInterval = defaultHeartbeatInterval
	}
	if o.readTimeout <= 0 {
		o.readTimeout = defaultReadTimeout
	}
	if o.maxMessageBytes <= 0 {
		o.maxMessageBytes = defaultMaxMessageBytes
	}
	if o.sendQueueSize <= 0 {
		o.sendQueueSize = defaultSendQueueSize
	}
	if o.handler == nil {
		o.handler = nopHandler{}
	}
}

type queuedMessage struct {
	ctx     context.Context
	message Envelope
	result  chan error
}

type connection struct {
	ws        *websocket.Conn
	nodeID    string
	sessionID string
	epoch     uint64
	opts      connectionOptions

	ctx    context.Context
	cancel context.CancelCauseFunc
	send   chan queuedMessage
	done   chan struct{}

	closeOnce sync.Once
	nextSeq   atomic.Uint64
	lastRecv  atomic.Uint64
}

func newConnection(parent context.Context, ws *websocket.Conn, nodeID, sessionID string, epoch uint64, opts connectionOptions) *connection {
	opts.setDefaults()
	ctx, cancel := context.WithCancelCause(parent)
	c := &connection{
		ws:        ws,
		nodeID:    nodeID,
		sessionID: sessionID,
		epoch:     epoch,
		opts:      opts,
		ctx:       ctx,
		cancel:    cancel,
		send:      make(chan queuedMessage, opts.sendQueueSize),
		done:      make(chan struct{}),
	}
	return c
}

func (c *connection) NodeID() string          { return c.nodeID }
func (c *connection) SessionID() string       { return c.sessionID }
func (c *connection) ConnectionEpoch() uint64 { return c.epoch }
func (c *connection) LastReceivedSeq() uint64 { return c.lastRecv.Load() }
func (c *connection) LastSentSeq() uint64     { return c.nextSeq.Load() }

func (c *connection) Send(ctx context.Context, message Envelope) error {
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan error, 1)
	queued := queuedMessage{ctx: ctx, message: message, result: result}
	select {
	case <-c.done:
		return c.closeCause()
	case <-ctx.Done():
		return ctx.Err()
	case c.send <- queued:
	}
	select {
	case <-c.done:
		return c.closeCause()
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

func (c *connection) run(initialRecvSeq, initialSendSeq uint64) error {
	c.lastRecv.Store(initialRecvSeq)
	c.nextSeq.Store(initialSendSeq)
	errCh := make(chan error, 2)
	go func() { errCh <- c.writeLoop() }()
	go func() { errCh <- c.readLoop() }()

	err := <-errCh
	c.close(err)
	<-errCh
	if cause := context.Cause(c.ctx); cause != nil {
		return cause
	}
	return err
}

func (c *connection) close(cause error) {
	if cause == nil {
		cause = ErrSessionClosed
	}
	c.closeOnce.Do(func() {
		c.cancel(cause)
		if errors.Is(cause, ErrSessionSuperseded) {
			_ = c.ws.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(closeSessionSuperseded, "session_superseded"),
				time.Now().Add(c.opts.writeTimeout),
			)
		}
		_ = c.ws.Close()
		close(c.done)
	})
}

func (c *connection) closeCause() error {
	cause := context.Cause(c.ctx)
	if cause == nil {
		return ErrSessionClosed
	}
	return cause
}

func (c *connection) writeLoop() error {
	ticker := time.NewTicker(c.opts.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return context.Cause(c.ctx)
		case queued := <-c.send:
			if err := queued.ctx.Err(); err != nil {
				queued.result <- err
				continue
			}
			err := c.write(queued.message)
			queued.result <- err
			if err != nil {
				return err
			}
		case now := <-ticker.C:
			message, err := NewEnvelope(TypeHeartbeat, Heartbeat{ObservedAt: now.UTC()})
			if err != nil {
				return err
			}
			if err := c.write(message); err != nil {
				return err
			}
		}
	}
}

func (c *connection) write(message Envelope) error {
	if message.ProtocolVersion == "" {
		message.ProtocolVersion = ProtocolVersionV1
	}
	if message.MessageID == "" {
		return errors.New("message_id is required")
	}
	if message.SentAt.IsZero() {
		message.SentAt = time.Now().UTC()
	}
	if message.NodeID == "" {
		message.NodeID = c.nodeID
	}
	if message.SessionID == "" {
		message.SessionID = c.sessionID
	}
	currentSeq := c.nextSeq.Load()
	if message.Seq == 0 {
		message.Seq = currentSeq + 1
	} else if message.Seq != currentSeq+1 {
		return fmt.Errorf("outgoing sequence must be %d, got %d", currentSeq+1, message.Seq)
	}
	c.nextSeq.Store(message.Seq)
	if err := validateTransportEnvelope(message); err != nil {
		return err
	}
	if err := c.ws.SetWriteDeadline(time.Now().Add(c.opts.writeTimeout)); err != nil {
		return err
	}
	if err := c.ws.WriteJSON(message); err != nil {
		return fmt.Errorf("write websocket message: %w", err)
	}
	return nil
}

func (c *connection) readLoop() error {
	c.ws.SetReadLimit(c.opts.maxMessageBytes)
	if err := c.refreshReadDeadline(); err != nil {
		return err
	}
	c.ws.SetPongHandler(func(string) error { return c.refreshReadDeadline() })
	for {
		var message Envelope
		if err := c.ws.ReadJSON(&message); err != nil {
			if websocket.IsCloseError(err, closeSessionSuperseded) {
				return ErrSessionSuperseded
			}
			return fmt.Errorf("read websocket message: %w", err)
		}
		if err := c.refreshReadDeadline(); err != nil {
			return err
		}
		if err := validateTransportEnvelope(message); err != nil {
			if nackErr := c.sendNack(message, "invalid_envelope", err); nackErr != nil {
				return nackErr
			}
			return err
		}
		if message.ProtocolVersion != ProtocolVersionV1 {
			return fmt.Errorf("unsupported protocol version %q", message.ProtocolVersion)
		}
		if message.NodeID != "" && message.NodeID != c.nodeID {
			return fmt.Errorf("message node_id %q does not match authenticated node %q", message.NodeID, c.nodeID)
		}
		if message.SessionID != "" && message.SessionID != c.sessionID {
			return fmt.Errorf("message session_id %q does not match active session %q", message.SessionID, c.sessionID)
		}
		expected := c.lastRecv.Load() + 1
		if message.Seq != expected {
			err := fmt.Errorf("expected seq %d, got %d", expected, message.Seq)
			if nackErr := c.sendNack(message, "sequence_gap", err); nackErr != nil {
				return nackErr
			}
			return err
		}
		c.lastRecv.Store(message.Seq)
		if message.Type == TypeAck || message.Type == TypeNack {
			if err := c.opts.handler.HandleMessage(c.ctx, c, message); err != nil {
				return err
			}
			continue
		}
		if err := c.opts.handler.HandleMessage(c.ctx, c, message); err != nil {
			if nackErr := c.sendNack(message, "handler_error", err); nackErr != nil {
				return nackErr
			}
			continue
		}
		ack, err := NewEnvelope(TypeAck, Ack{MessageID: message.MessageID, AckSeq: message.Seq})
		if err != nil {
			return err
		}
		ack.CorrelationID = message.MessageID
		if err := c.Send(c.ctx, ack); err != nil {
			return err
		}
	}
}

func (c *connection) sendNack(message Envelope, code string, cause error) error {
	nack, err := NewEnvelope(TypeNack, Nack{
		MessageID: message.MessageID,
		AckSeq:    c.lastRecv.Load(),
		Code:      code,
		Error:     cause.Error(),
		Retryable: code == "handler_error",
	})
	if err != nil {
		return err
	}
	nack.CorrelationID = message.MessageID
	if err := c.Send(c.ctx, nack); err != nil {
		return err
	}
	return nil
}

func (c *connection) refreshReadDeadline() error {
	return c.ws.SetReadDeadline(time.Now().Add(c.opts.readTimeout))
}
