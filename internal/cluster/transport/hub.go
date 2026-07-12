package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type AuthenticateFunc func(context.Context, *http.Request, Hello) error

type HubOptions struct {
	CoordinatorID       string
	Authenticate        AuthenticateFunc
	CheckOrigin         func(*http.Request) bool
	Handler             Handler
	OnConnect           func(Peer)
	OnDisconnect        func(Peer, error)
	HandshakeTimeout    time.Duration
	WriteTimeout        time.Duration
	HeartbeatInterval   time.Duration
	LeaseDuration       time.Duration
	ReadTimeout         time.Duration
	MaxMessageBytes     int64
	SendQueueSize       int
	RejectDuplicateNode bool
}

// Hub accepts authenticated Worker WebSocket sessions and guarantees that a
// node has at most one active connection epoch.
type Hub struct {
	opts     HubOptions
	upgrader websocket.Upgrader

	mu       sync.RWMutex
	sessions map[string]*connection
	epochs   map[string]uint64
	closed   bool
}

func NewHub(opts HubOptions) *Hub {
	if opts.CoordinatorID == "" {
		opts.CoordinatorID = uuid.NewString()
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultWriteTimeout
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	checkOrigin := opts.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = func(*http.Request) bool { return true }
	}
	return &Hub{
		opts: opts,
		upgrader: websocket.Upgrader{
			CheckOrigin: checkOrigin,
		},
		sessions: make(map[string]*connection),
		epochs:   make(map[string]uint64),
	}
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	if err := ws.SetReadDeadline(time.Now().Add(h.opts.HandshakeTimeout)); err != nil {
		return
	}
	ws.SetReadLimit(h.maxMessageBytes())
	var helloMessage Envelope
	if err := ws.ReadJSON(&helloMessage); err != nil {
		h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "hello required")
		return
	}
	if err := validateTransportEnvelope(helloMessage); err != nil || helloMessage.Type != TypeHello || helloMessage.ProtocolVersion != ProtocolVersionV1 {
		h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "invalid hello")
		return
	}
	hello, err := decodePayload[Hello](helloMessage)
	if err != nil || hello.NodeID == "" {
		h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "invalid hello payload")
		return
	}
	if !containsVersion(hello.SupportedVersions, ProtocolVersionV1) {
		h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "protocol version not supported by worker")
		return
	}
	if helloMessage.NodeID != "" && helloMessage.NodeID != hello.NodeID {
		h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "node identity mismatch")
		return
	}
	if h.opts.Authenticate != nil {
		if err := h.opts.Authenticate(r.Context(), r, hello); err != nil {
			h.writeHandshakeClose(ws, websocket.ClosePolicyViolation, "authentication failed")
			return
		}
	}

	session, previous, err := h.register(r.Context(), ws, hello.NodeID)
	if err != nil {
		h.writeHandshakeClose(ws, websocket.CloseTryAgainLater, err.Error())
		return
	}
	if previous != nil {
		previous.close(ErrSessionSuperseded)
	}
	defer h.unregister(session)

	welcome, err := NewEnvelope(TypeWelcome, Welcome{
		CoordinatorID:    h.opts.CoordinatorID,
		NodeID:           hello.NodeID,
		SessionID:        session.sessionID,
		ProtocolVersion:  ProtocolVersionV1,
		ConnectionEpoch:  session.epoch,
		HeartbeatSeconds: int(session.opts.heartbeatInterval / time.Second),
		LeaseSeconds:     int(h.opts.LeaseDuration / time.Second),
		ServerTime:       time.Now().UTC(),
	})
	if err != nil {
		session.close(err)
		return
	}
	welcome.NodeID = hello.NodeID
	welcome.SessionID = session.sessionID
	welcome.Seq = 1
	if err := session.write(welcome); err != nil {
		session.close(err)
		return
	}
	runDone := make(chan error, 1)
	go func() { runDone <- session.run(helloMessage.Seq, welcome.Seq) }()
	if h.opts.OnConnect != nil {
		h.opts.OnConnect(session)
	}
	runErr := <-runDone
	if h.opts.OnDisconnect != nil {
		h.opts.OnDisconnect(session, runErr)
	}
}

func (h *Hub) Send(ctx context.Context, nodeID string, message Envelope) error {
	h.mu.RLock()
	session := h.sessions[nodeID]
	h.mu.RUnlock()
	if session == nil {
		return ErrNotConnected
	}
	return session.Send(ctx, message)
}

func (h *Hub) Session(nodeID string) (Peer, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	session, ok := h.sessions[nodeID]
	return session, ok
}

func (h *Hub) ConnectedNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	nodes := make([]string, 0, len(h.sessions))
	for nodeID := range h.sessions {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

func (h *Hub) Disconnect(nodeID string, cause error) bool {
	h.mu.RLock()
	session := h.sessions[nodeID]
	h.mu.RUnlock()
	if session == nil {
		return false
	}
	session.close(cause)
	return true
}

func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	sessions := make([]*connection, 0, len(h.sessions))
	for _, session := range h.sessions {
		sessions = append(sessions, session)
	}
	h.sessions = make(map[string]*connection)
	h.mu.Unlock()
	for _, session := range sessions {
		session.close(ErrSessionClosed)
	}
	return nil
}

func (h *Hub) register(parent context.Context, ws *websocket.Conn, nodeID string) (*connection, *connection, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, errors.New("cluster transport hub is closed")
	}
	if h.opts.RejectDuplicateNode && h.sessions[nodeID] != nil {
		return nil, nil, errors.New("cluster node already has an active session")
	}
	epoch := h.epochs[nodeID] + 1
	h.epochs[nodeID] = epoch
	sessionID := uuid.NewString()
	session := newConnection(parent, ws, nodeID, sessionID, epoch, connectionOptions{
		writeTimeout:      h.opts.WriteTimeout,
		heartbeatInterval: h.opts.HeartbeatInterval,
		readTimeout:       h.opts.ReadTimeout,
		maxMessageBytes:   h.opts.MaxMessageBytes,
		sendQueueSize:     h.opts.SendQueueSize,
		handler:           h.opts.Handler,
	})
	previous := h.sessions[nodeID]
	h.sessions[nodeID] = session
	return session, previous, nil
}

func (h *Hub) unregister(session *connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessions[session.nodeID] == session {
		delete(h.sessions, session.nodeID)
	}
}

func (h *Hub) maxMessageBytes() int64 {
	if h.opts.MaxMessageBytes > 0 {
		return h.opts.MaxMessageBytes
	}
	return defaultMaxMessageBytes
}

func (h *Hub) writeHandshakeClose(ws *websocket.Conn, code int, reason string) {
	deadline := time.Now().Add(h.opts.HandshakeTimeout)
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
}

func (h *Hub) String() string {
	return fmt.Sprintf("cluster transport hub %s", h.opts.CoordinatorID)
}

func containsVersion(versions []string, target string) bool {
	for _, version := range versions {
		if version == target {
			return true
		}
	}
	return false
}
