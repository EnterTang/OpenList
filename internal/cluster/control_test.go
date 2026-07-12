package cluster

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorSecretEncryptionDoesNotPersistPlaintext(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Cluster.SecretMasterKey = strings.Repeat("42", 32)
	t.Cleanup(func() { conf.Conf = original })

	plaintext := []byte(`{"authorization":"Bearer very-secret-token"}`)
	ciphertext, nonce, fingerprint, err := encryptCoordinatorSecret(plaintext)
	require.NoError(t, err)
	require.NotContains(t, ciphertext, "very-secret-token")
	require.NotEmpty(t, nonce)
	require.Len(t, fingerprint, 64)

	recovered, err := decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: ciphertext, Nonce: nonce})
	require.NoError(t, err)
	require.True(t, bytes.Equal(plaintext, recovered))

	tampered, err := base64.RawStdEncoding.DecodeString(ciphertext)
	require.NoError(t, err)
	tampered[0] ^= 0x01
	_, err = decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: base64.RawStdEncoding.EncodeToString(tampered), Nonce: nonce})
	require.Error(t, err)
}
