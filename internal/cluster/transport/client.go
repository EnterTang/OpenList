package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	clusterprotocol "github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/gorilla/websocket"
)

type WorkerClientOptions struct {
	URL               string
	NodeID            string
	NodeName          string
	AgentVersion      string
	Role              string
	Labels            map[string]string
	SupportedVersions []string
	EnrollmentToken   string
	Header            http.Header
	Dialer            *websocket.Dialer
	Handler           Handler
	HandshakeTimeout  time.Duration
	WriteTimeout      time.Duration
	HeartbeatInterval time.Duration
	ReadTimeout       time.Duration
	MaxMessageBytes   int64
	SendQueueSize     int
	ReconnectMinDelay time.Duration
	ReconnectMaxDelay time.Duration
	ResumeSessionID   string
	LastReceivedSeq   uint64
	HelloControlState func() (*clusterprotocol.NodeKeyAgreement, uint64)
	OnConnect         func(Peer, Welcome)
	OnDisconnect      func(error)
}

// WorkerClient owns the Worker side of the cluster WebSocket. Run reconnects
// until its context is cancelled; RunOnce is available to callers that want
// to own retry policy themselves.
type WorkerClient struct {
	opts WorkerClientOptions

	mu      sync.RWMutex
	current *connection
	welcome Welcome
}

func NewWorkerClient(opts WorkerClientOptions) (*WorkerClient, error) {
	if opts.URL == "" {
		return nil, errors.New("cluster coordinator URL is required")
	}
	if opts.NodeID == "" {
		return nil, errors.New("cluster worker node ID is required")
	}
	if opts.NodeName == "" {
		opts.NodeName = opts.NodeID
	}
	if opts.Role == "" {
		opts.Role = "worker"
	}
	if len(opts.SupportedVersions) == 0 {
		opts.SupportedVersions = []string{ProtocolVersionV1}
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultWriteTimeout
	}
	if opts.ReconnectMinDelay <= 0 {
		opts.ReconnectMinDelay = time.Second
	}
	if opts.ReconnectMaxDelay <= 0 {
		opts.ReconnectMaxDelay = 30 * time.Second
	}
	if opts.ReconnectMaxDelay < opts.ReconnectMinDelay {
		opts.ReconnectMaxDelay = opts.ReconnectMinDelay
	}
	return &WorkerClient{opts: opts}, nil
}

func (c *WorkerClient) Run(ctx context.Context) error {
	delay := c.opts.ReconnectMinDelay
	for {
		_ = c.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > c.opts.ReconnectMaxDelay {
			delay = c.opts.ReconnectMaxDelay
		}
	}
}

func (c *WorkerClient) RunOnce(ctx context.Context) error {
	dialer := c.opts.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	dialCtx, cancel := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancel()
	ws, response, err := dialer.DialContext(dialCtx, c.opts.URL, c.opts.Header.Clone())
	if err != nil {
		if response != nil {
			return fmt.Errorf("dial cluster coordinator: %w (status %s)", err, response.Status)
		}
		return fmt.Errorf("dial cluster coordinator: %w", err)
	}
	defer ws.Close()

	var keyAgreement *clusterprotocol.NodeKeyAgreement
	var observedRevision uint64
	if c.opts.HelloControlState != nil {
		keyAgreement, observedRevision = c.opts.HelloControlState()
	}
	hello, err := NewEnvelope(TypeHello, Hello{
		NodeID:            c.opts.NodeID,
		NodeName:          c.opts.NodeName,
		AgentVersion:      c.opts.AgentVersion,
		SupportedVersions: c.opts.SupportedVersions,
		Role:              c.opts.Role,
		ResumeSessionID:   c.opts.ResumeSessionID,
		LastReceivedSeq:   c.opts.LastReceivedSeq,
		EnrollmentToken:   c.opts.EnrollmentToken,
		Labels:            c.opts.Labels,
		KeyAgreement:      keyAgreement,
		ObservedRevision:  observedRevision,
	})
	if err != nil {
		return err
	}
	hello.NodeID = c.opts.NodeID
	hello.Seq = 1
	if err := ws.SetWriteDeadline(time.Now().Add(c.opts.HandshakeTimeout)); err != nil {
		return err
	}
	if err := ws.WriteJSON(hello); err != nil {
		return fmt.Errorf("write cluster hello: %w", err)
	}
	if err := ws.SetReadDeadline(time.Now().Add(c.opts.HandshakeTimeout)); err != nil {
		return err
	}
	var welcomeMessage Envelope
	if err := ws.ReadJSON(&welcomeMessage); err != nil {
		return fmt.Errorf("read cluster welcome: %w", err)
	}
	if err := validateTransportEnvelope(welcomeMessage); err != nil {
		return fmt.Errorf("validate cluster welcome: %w", err)
	}
	if welcomeMessage.Type != TypeWelcome || welcomeMessage.ProtocolVersion != ProtocolVersionV1 {
		return fmt.Errorf("unexpected cluster handshake message %q", welcomeMessage.Type)
	}
	welcome, err := decodePayload[Welcome](welcomeMessage)
	if err != nil {
		return err
	}
	if welcome.SessionID == "" || welcome.ConnectionEpoch == 0 {
		return errors.New("cluster welcome is missing session identity")
	}
	if welcome.NodeID != c.opts.NodeID || welcome.ProtocolVersion != ProtocolVersionV1 {
		return errors.New("cluster welcome does not match worker identity or protocol")
	}

	opts := connectionOptions{
		writeTimeout:      c.opts.WriteTimeout,
		heartbeatInterval: c.opts.HeartbeatInterval,
		readTimeout:       c.opts.ReadTimeout,
		maxMessageBytes:   c.opts.MaxMessageBytes,
		sendQueueSize:     c.opts.SendQueueSize,
		handler:           c.opts.Handler,
	}
	if opts.heartbeatInterval <= 0 && welcome.HeartbeatSeconds > 0 {
		opts.heartbeatInterval = time.Duration(welcome.HeartbeatSeconds) * time.Second
	}
	conn := newConnection(ctx, ws, c.opts.NodeID, welcome.SessionID, welcome.ConnectionEpoch, opts)
	c.setCurrent(conn, welcome)
	defer c.clearCurrent(conn)
	runDone := make(chan error, 1)
	go func() { runDone <- conn.run(welcomeMessage.Seq, hello.Seq) }()
	if c.opts.OnConnect != nil {
		c.opts.OnConnect(conn, welcome)
	}
	runErr := <-runDone
	if c.opts.OnDisconnect != nil {
		c.opts.OnDisconnect(runErr)
	}
	return runErr
}

func (c *WorkerClient) Send(ctx context.Context, message Envelope) error {
	c.mu.RLock()
	current := c.current
	c.mu.RUnlock()
	if current == nil {
		return ErrNotConnected
	}
	return current.Send(ctx, message)
}

func (c *WorkerClient) Peer() (Peer, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return nil, false
	}
	return c.current, true
}

func (c *WorkerClient) Welcome() (Welcome, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.welcome, c.current != nil
}

func (c *WorkerClient) Close() error {
	c.mu.RLock()
	current := c.current
	c.mu.RUnlock()
	if current != nil {
		current.close(ErrSessionClosed)
	}
	return nil
}

func (c *WorkerClient) setCurrent(conn *connection, welcome Welcome) {
	c.mu.Lock()
	previous := c.current
	c.current = conn
	c.welcome = welcome
	c.mu.Unlock()
	if previous != nil && previous != conn {
		previous.close(ErrSessionSuperseded)
	}
}

func (c *WorkerClient) clearCurrent(conn *connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == conn {
		c.current = nil
		c.welcome = Welcome{}
	}
}
