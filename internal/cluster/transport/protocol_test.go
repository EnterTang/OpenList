package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	message, err := NewEnvelope("inventory.report", map[string]string{"driver": "139"})
	require.NoError(t, err)
	message.Seq = 1
	require.NoError(t, validateTransportEnvelope(message))

	payload, err := decodePayload[map[string]string](message)
	require.NoError(t, err)
	require.Equal(t, "139", payload["driver"])
}

func TestEnvelopeValidateRequiresTransportIdentity(t *testing.T) {
	t.Parallel()

	message, err := NewEnvelope("job.offer", struct{}{})
	require.NoError(t, err)
	require.ErrorContains(t, validateTransportEnvelope(message), "seq")

	message.Seq = 1
	message.MessageID = ""
	require.Error(t, validateTransportEnvelope(message))
}
