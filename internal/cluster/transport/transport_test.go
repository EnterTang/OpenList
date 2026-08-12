package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHubAndWorkerClientExchangeMessageAndAck(t *testing.T) {
	t.Parallel()

	hubReceived := make(chan Envelope, 1)
	clientReceived := make(chan Envelope, 2)
	connected := make(chan Peer, 1)
	clientConnected := make(chan Peer, 1)
	authenticated := make(chan Hello, 1)
	hub := NewHub(HubOptions{
		CoordinatorID:     "coordinator-1",
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
		Authenticate: func(_ context.Context, _ *http.Request, hello Hello) error {
			authenticated <- hello
			return nil
		},
		Handler: HandlerFunc(func(_ context.Context, _ Peer, message Envelope) error {
			hubReceived <- message
			return nil
		}),
		OnConnect: func(peer Peer) { connected <- peer },
	})
	server := httptest.NewServer(hub)
	t.Cleanup(func() {
		server.Close()
		require.NoError(t, hub.Close())
	})

	client, err := NewWorkerClient(WorkerClientOptions{
		URL:               websocketURL(server.URL),
		NodeID:            "worker-1",
		NodeName:          "Shanghai worker",
		AgentVersion:      "test",
		Labels:            map[string]string{"carrier": "telecom"},
		EnrollmentToken:   "enroll-once",
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
		Handler: HandlerFunc(func(_ context.Context, _ Peer, message Envelope) error {
			clientReceived <- message
			return nil
		}),
		OnConnect: func(peer Peer, _ Welcome) { clientConnected <- peer },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.RunOnce(ctx) }()

	peer := receive(t, connected)
	receive(t, clientConnected)
	hello := receive(t, authenticated)
	require.Equal(t, "Shanghai worker", hello.NodeName)
	require.Equal(t, "telecom", hello.Labels["carrier"])
	require.Equal(t, "enroll-once", hello.EnrollmentToken)
	require.Contains(t, hello.SupportedVersions, ProtocolVersionV1)
	require.Equal(t, "worker-1", peer.NodeID())
	require.Equal(t, uint64(1), peer.ConnectionEpoch())

	message, err := NewEnvelope("inventory.report", map[string]string{"mount": "/mobile"})
	require.NoError(t, err)
	require.NoError(t, client.Send(context.Background(), message))

	received := receive(t, hubReceived)
	require.Equal(t, "inventory.report", string(received.Type))
	require.Equal(t, message.MessageID, received.MessageID)

	ackMessage := receive(t, clientReceived)
	require.Equal(t, TypeAck, ackMessage.Type)
	ack, err := decodePayload[Ack](ackMessage)
	require.NoError(t, err)
	require.Equal(t, message.MessageID, ack.MessageID)
	require.Equal(t, received.Seq, ack.AckSeq)

	cancel()
	require.ErrorIs(t, receive(t, clientDone), context.Canceled)
}

func TestHandlerErrorReturnsNackWithoutClosingSession(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	clientReceived := make(chan Envelope, 2)
	connected := make(chan Peer, 1)
	clientConnected := make(chan Peer, 1)
	hub := NewHub(HubOptions{
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
		Handler: HandlerFunc(func(_ context.Context, _ Peer, _ Envelope) error {
			if calls.Add(1) == 1 {
				return errors.New("persist failed")
			}
			return nil
		}),
		OnConnect: func(peer Peer) { connected <- peer },
	})
	server := httptest.NewServer(hub)
	t.Cleanup(func() {
		server.Close()
		require.NoError(t, hub.Close())
	})

	client, err := NewWorkerClient(WorkerClientOptions{
		URL:               websocketURL(server.URL),
		NodeID:            "worker-nack",
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
		Handler: HandlerFunc(func(_ context.Context, _ Peer, message Envelope) error {
			clientReceived <- message
			return nil
		}),
		OnConnect: func(peer Peer, _ Welcome) { clientConnected <- peer },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.RunOnce(ctx) }()
	receive(t, connected)
	receive(t, clientConnected)

	first, err := NewEnvelope("job.offer", struct{}{})
	require.NoError(t, err)
	require.NoError(t, client.Send(context.Background(), first))
	nackMessage := receive(t, clientReceived)
	require.Equal(t, TypeNack, nackMessage.Type)
	nack, err := decodePayload[Nack](nackMessage)
	require.NoError(t, err)
	require.Equal(t, "handler_error", nack.Code)

	second, err := NewEnvelope("inventory.report", struct{}{})
	require.NoError(t, err)
	require.NoError(t, client.Send(context.Background(), second))
	ackMessage := receive(t, clientReceived)
	require.Equal(t, TypeAck, ackMessage.Type)

	cancel()
	require.ErrorIs(t, receive(t, clientDone), context.Canceled)
}

func TestNewWorkerSessionSupersedesPreviousEpoch(t *testing.T) {
	t.Parallel()

	connected := make(chan Peer, 2)
	hub := NewHub(HubOptions{
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
		OnConnect:         func(peer Peer) { connected <- peer },
	})
	server := httptest.NewServer(hub)
	t.Cleanup(func() {
		server.Close()
		require.NoError(t, hub.Close())
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client1 := newTestClient(t, server.URL, "same-worker")
	client1Done := make(chan error, 1)
	go func() { client1Done <- client1.RunOnce(ctx) }()
	first := receive(t, connected)
	require.Equal(t, uint64(1), first.ConnectionEpoch())

	client2 := newTestClient(t, server.URL, "same-worker")
	client2Done := make(chan error, 1)
	go func() { client2Done <- client2.RunOnce(ctx) }()
	second := receive(t, connected)
	require.Equal(t, uint64(2), second.ConnectionEpoch())
	require.NotEqual(t, first.SessionID(), second.SessionID())
	require.ErrorIs(t, receive(t, client1Done), ErrSessionSuperseded)

	active, ok := hub.Session("same-worker")
	require.True(t, ok)
	require.Equal(t, second.SessionID(), active.SessionID())

	cancel()
	require.ErrorIs(t, receive(t, client2Done), context.Canceled)
}

func newTestClient(t *testing.T, serverURL, nodeID string) *WorkerClient {
	t.Helper()
	client, err := NewWorkerClient(WorkerClientOptions{
		URL:               websocketURL(serverURL),
		NodeID:            nodeID,
		HeartbeatInterval: time.Hour,
		ReadTimeout:       time.Hour,
	})
	require.NoError(t, err)
	return client
}

func websocketURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for channel value")
		var zero T
		return zero
	}
}
