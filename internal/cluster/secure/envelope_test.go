package secure

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeRoundTripAndAuthentication(t *testing.T) {
	worker, err := GenerateKeyPair()
	require.NoError(t, err)
	aad := []byte("node-1\x00revision-7")
	sealed, err := SealJSON(worker.PublicKey(), map[string]any{"refresh_token": "secret-token"}, aad)
	require.NoError(t, err)
	require.NotContains(t, sealed, "secret-token")

	var got map[string]any
	require.NoError(t, worker.OpenJSON(sealed, aad, &got))
	require.Equal(t, "secret-token", got["refresh_token"])

	other, err := GenerateKeyPair()
	require.NoError(t, err)
	require.ErrorContains(t, other.OpenJSON(sealed, aad, &got), "recipient")
	require.ErrorContains(t, worker.OpenJSON(sealed, []byte("other-node"), &got), "authentication failed")
}

func TestEnvelopeRejectsCiphertextTampering(t *testing.T) {
	worker, err := GenerateKeyPair()
	require.NoError(t, err)
	sealed, err := Seal(worker.PublicKey(), []byte("credential"), []byte("aad"))
	require.NoError(t, err)
	var envelope Envelope
	require.NoError(t, json.Unmarshal([]byte(sealed), &envelope))
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	require.NoError(t, err)
	ciphertext[0] ^= 0x01
	envelope.Ciphertext = base64.RawStdEncoding.EncodeToString(ciphertext)
	tampered, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = worker.Open(string(tampered), []byte("aad"))
	require.Error(t, err)
}

func TestLoadOrCreateKeyPairPersistsPrivateKeyWithRestrictedMode(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "cluster", "worker.key")
	first, err := LoadOrCreateKeyPair(filename)
	require.NoError(t, err)
	second, err := LoadOrCreateKeyPair(filename)
	require.NoError(t, err)
	require.Equal(t, first.KeyID(), second.KeyID())
	info, err := os.Stat(filename)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}
