package cluster

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	clusterdb "github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
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

func TestCoordinatorSecretDecryptsWithPreviousKeyAndWritesWithCurrentKey(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	currentKey := strings.Repeat("22", 32)
	previousKey := strings.Repeat("11", 32)
	conf.Conf.Cluster.SecretMasterKey = currentKey
	conf.Conf.Cluster.SecretMasterKeyPrevious = previousKey
	t.Cleanup(func() { conf.Conf = original })

	plaintext := []byte(`{"username":"qb-user","password":"qb-password"}`)
	ciphertext, nonce := encryptTestSecret(t, previousKey, plaintext)
	recovered, err := decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: ciphertext, Nonce: nonce})
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)

	currentCiphertext, currentNonce, _, err := encryptCoordinatorSecret(plaintext)
	require.NoError(t, err)
	conf.Conf.Cluster.SecretMasterKeyPrevious = ""
	recovered, err = decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: currentCiphertext, Nonce: currentNonce})
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)

	conf.Conf.Cluster.SecretMasterKey = previousKey
	_, err = decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: currentCiphertext, Nonce: currentNonce})
	require.Error(t, err, "newly written ciphertext must not depend on the previous key")
}

func TestCoordinatorSecretAcceptsLegacyAES128PreviousKey(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Cluster.SecretMasterKey = strings.Repeat("22", 32)
	conf.Conf.Cluster.SecretMasterKeyPrevious = strings.Repeat("11", 16)
	t.Cleanup(func() { conf.Conf = original })

	plaintext := []byte(`{"legacy":"secret"}`)
	ciphertext, nonce := encryptTestSecret(t, conf.Conf.Cluster.SecretMasterKeyPrevious, plaintext)
	recovered, err := decryptCoordinatorSecret(model.ClusterSecret{Ciphertext: ciphertext, Nonce: nonce})
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)
}

func TestCoordinatorSecretRejectsLegacyAES128CurrentKey(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Cluster.SecretMasterKey = strings.Repeat("11", 16)
	t.Cleanup(func() { conf.Conf = original })

	_, _, _, err := encryptCoordinatorSecret([]byte(`{"secret":"value"}`))
	require.EqualError(t, err, "cluster secret master key must be 32 bytes encoded as hex or base64")
}

func TestCoordinatorSecretMigrationReencryptsPreviousKeyRowsAtomically(t *testing.T) {
	originalConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	currentKey := strings.Repeat("22", 32)
	previousKey := strings.Repeat("11", 32)
	conf.Conf.Cluster.SecretMasterKey = currentKey
	conf.Conf.Cluster.SecretMasterKeyPrevious = previousKey
	t.Cleanup(func() { conf.Conf = originalConf })

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:cluster_secret_migration_%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.ClusterSecret{}, &model.ClusterControlAudit{}))
	originalDB := clusterdb.GetDb()
	clusterdb.UseConnection(database)
	t.Cleanup(func() {
		clusterdb.UseConnection(originalDB)
		sqlDB, dbErr := database.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	plaintext := []byte(`{"hmac_key":"bridge-secret"}`)
	ciphertext, nonce := encryptTestSecret(t, previousKey, plaintext)
	createdAt := time.Now().UTC().Add(-time.Hour)
	secret := &model.ClusterSecret{
		ID: "secret-previous", CreatedAt: createdAt, UpdatedAt: createdAt, Alias: "bridge",
		Kind: "moviepilot_bridge_hmac", Ciphertext: ciphertext, Nonce: nonce,
		Fingerprint: fmt.Sprintf("%x", sha256Sum(plaintext)), Version: 1, RotatedAt: createdAt,
	}
	require.NoError(t, database.Create(secret).Error)

	result, err := MigrateSecrets(context.Background(), ControlActor{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.Equal(t, 1, result.Migrated)
	require.Zero(t, result.Skipped)

	conf.Conf.Cluster.SecretMasterKeyPrevious = ""
	recovered, _, err := ResolveSecret(context.Background(), secret.ID)
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)

	var migrated model.ClusterSecret
	require.NoError(t, database.First(&migrated, "id = ?", secret.ID).Error)
	require.Equal(t, uint64(2), migrated.Version)
	require.NotEqual(t, ciphertext, migrated.Ciphertext)
	var audit model.ClusterControlAudit
	require.NoError(t, database.First(&audit, "action = ?", "secret.migrate").Error)
	require.Equal(t, "succeeded", audit.Outcome)
}

func TestCoordinatorSecretMigrationSkipsRowsAlreadyUsingCurrentKey(t *testing.T) {
	originalConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	currentKey := strings.Repeat("22", 32)
	previousKey := strings.Repeat("11", 32)
	conf.Conf.Cluster.SecretMasterKey = currentKey
	conf.Conf.Cluster.SecretMasterKeyPrevious = previousKey
	t.Cleanup(func() { conf.Conf = originalConf })

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:cluster_secret_migration_skip_%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.ClusterSecret{}, &model.ClusterControlAudit{}))
	originalDB := clusterdb.GetDb()
	clusterdb.UseConnection(database)
	t.Cleanup(func() {
		clusterdb.UseConnection(originalDB)
		sqlDB, dbErr := database.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	createdAt := time.Now().UTC()
	oldCiphertext, oldNonce := encryptTestSecret(t, previousKey, []byte(`{"source":"previous"}`))
	currentCiphertext, currentNonce := encryptTestSecret(t, currentKey, []byte(`{"source":"current"}`))
	require.NoError(t, database.Create(&model.ClusterSecret{
		ID: "secret-previous", CreatedAt: createdAt, UpdatedAt: createdAt, Alias: "previous", Kind: "test",
		Ciphertext: oldCiphertext, Nonce: oldNonce, Version: 1, RotatedAt: createdAt,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterSecret{
		ID: "secret-current", CreatedAt: createdAt, UpdatedAt: createdAt, Alias: "current", Kind: "test",
		Ciphertext: currentCiphertext, Nonce: currentNonce, Version: 1, RotatedAt: createdAt,
	}).Error)

	result, err := MigrateSecrets(context.Background(), ControlActor{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, 2, result.Total)
	require.Equal(t, 1, result.Migrated)
	require.Equal(t, 1, result.Skipped)

	var current model.ClusterSecret
	require.NoError(t, database.First(&current, "id = ?", "secret-current").Error)
	require.Equal(t, uint64(1), current.Version)
	require.Equal(t, currentCiphertext, current.Ciphertext)
}

func TestCoordinatorSecretMigrationRollsBackWhenAnySecretCannotBeDecrypted(t *testing.T) {
	originalConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	currentKey := strings.Repeat("22", 32)
	previousKey := strings.Repeat("11", 32)
	conf.Conf.Cluster.SecretMasterKey = currentKey
	conf.Conf.Cluster.SecretMasterKeyPrevious = previousKey
	t.Cleanup(func() { conf.Conf = originalConf })

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:cluster_secret_migration_rollback_%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.ClusterSecret{}, &model.ClusterControlAudit{}))
	originalDB := clusterdb.GetDb()
	clusterdb.UseConnection(database)
	t.Cleanup(func() {
		clusterdb.UseConnection(originalDB)
		sqlDB, dbErr := database.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	plaintext := []byte(`{"token":"keep-old"}`)
	ciphertext, nonce := encryptTestSecret(t, previousKey, plaintext)
	createdAt := time.Now().UTC()
	require.NoError(t, database.Create(&model.ClusterSecret{
		ID: "secret-good", CreatedAt: createdAt, UpdatedAt: createdAt, Alias: "good", Kind: "test",
		Ciphertext: ciphertext, Nonce: nonce, Fingerprint: fmt.Sprintf("%x", sha256Sum(plaintext)), Version: 1, RotatedAt: createdAt,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterSecret{
		ID: "secret-bad", CreatedAt: createdAt, UpdatedAt: createdAt, Alias: "bad", Kind: "test",
		Ciphertext: "not-valid", Nonce: "not-valid", Fingerprint: "fingerprint", Version: 1, RotatedAt: createdAt,
	}).Error)

	_, err = MigrateSecrets(context.Background(), ControlActor{Name: "test"})
	require.Error(t, err)

	var unchanged model.ClusterSecret
	require.NoError(t, database.First(&unchanged, "id = ?", "secret-good").Error)
	require.Equal(t, ciphertext, unchanged.Ciphertext)
	require.Equal(t, uint64(1), unchanged.Version)
	var auditCount int64
	require.NoError(t, database.Model(&model.ClusterControlAudit{}).Where("action = ?", "secret.migrate").Count(&auditCount).Error)
	require.Zero(t, auditCount)
}

func encryptTestSecret(t *testing.T, encodedKey string, plaintext []byte) (string, string) {
	t.Helper()
	key, err := hex.DecodeString(encodedKey)
	require.NoError(t, err)
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonce := make([]byte, aead.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	require.NoError(t, err)
	ciphertext := aead.Seal(nil, nonce, plaintext, []byte("openlist-cluster-secret-v1"))
	return base64.RawStdEncoding.EncodeToString(ciphertext), base64.RawStdEncoding.EncodeToString(nonce)
}

func sha256Sum(data []byte) [32]byte {
	return sha256.Sum256(data)
}
