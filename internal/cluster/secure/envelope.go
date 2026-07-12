package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/crypto/hkdf"
)

const (
	EnvelopeVersion = "x25519-hkdf-sha256-aes256gcm-v1"
	keySize         = 32
)

type KeyPair struct {
	private *ecdh.PrivateKey
}

type Envelope struct {
	Version            string `json:"version"`
	RecipientKeyID     string `json:"recipient_key_id"`
	EphemeralPublicKey string `json:"ephemeral_public_key"`
	Salt               string `json:"salt"`
	Nonce              string `json:"nonce"`
	Ciphertext         string `json:"ciphertext"`
}

func GenerateKeyPair() (*KeyPair, error) {
	private, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate X25519 key: %w", err)
	}
	return &KeyPair{private: private}, nil
}

func NewKeyPair(privateKey []byte) (*KeyPair, error) {
	private, err := ecdh.X25519().NewPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("load X25519 private key: %w", err)
	}
	return &KeyPair{private: private}, nil
}

func (k *KeyPair) PrivateBytes() []byte {
	if k == nil || k.private == nil {
		return nil
	}
	return append([]byte(nil), k.private.Bytes()...)
}

func (k *KeyPair) PublicKey() string {
	if k == nil || k.private == nil {
		return ""
	}
	return base64.RawStdEncoding.EncodeToString(k.private.PublicKey().Bytes())
}

func (k *KeyPair) KeyID() string {
	if k == nil || k.private == nil {
		return ""
	}
	return Fingerprint(k.private.PublicKey().Bytes())
}

func Fingerprint(publicKey []byte) string {
	sum := sha256.Sum256(publicKey)
	return hex.EncodeToString(sum[:])
}

func SealJSON(publicKey string, value any, aad []byte) (string, error) {
	plaintext, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal secret payload: %w", err)
	}
	return Seal(publicKey, plaintext, aad)
}

func Seal(encodedPublicKey string, plaintext, aad []byte) (string, error) {
	recipientRaw, err := decodeKey(encodedPublicKey)
	if err != nil {
		return "", err
	}
	recipient, err := ecdh.X25519().NewPublicKey(recipientRaw)
	if err != nil {
		return "", fmt.Errorf("load recipient X25519 public key: %w", err)
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate ephemeral X25519 key: %w", err)
	}
	shared, err := ephemeral.ECDH(recipient)
	if err != nil {
		return "", fmt.Errorf("derive X25519 shared secret: %w", err)
	}
	salt := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", fmt.Errorf("generate envelope salt: %w", err)
	}
	key, err := deriveKey(shared, salt, aad)
	if err != nil {
		return "", err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate envelope nonce: %w", err)
	}
	envelope := Envelope{
		Version:            EnvelopeVersion,
		RecipientKeyID:     Fingerprint(recipientRaw),
		EphemeralPublicKey: base64.RawStdEncoding.EncodeToString(ephemeral.PublicKey().Bytes()),
		Salt:               base64.RawStdEncoding.EncodeToString(salt),
		Nonce:              base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext:         base64.RawStdEncoding.EncodeToString(aead.Seal(nil, nonce, plaintext, aad)),
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("marshal secret envelope: %w", err)
	}
	return string(raw), nil
}

func (k *KeyPair) OpenJSON(encoded string, aad []byte, dst any) error {
	plaintext, err := k.Open(encoded, aad)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(plaintext, dst); err != nil {
		return fmt.Errorf("decode secret payload: %w", err)
	}
	return nil
}

func (k *KeyPair) Open(encoded string, aad []byte) ([]byte, error) {
	if k == nil || k.private == nil {
		return nil, errors.New("worker X25519 private key is unavailable")
	}
	var envelope Envelope
	if err := json.Unmarshal([]byte(encoded), &envelope); err != nil {
		return nil, errors.New("invalid secret envelope")
	}
	if envelope.Version != EnvelopeVersion {
		return nil, fmt.Errorf("unsupported secret envelope version %q", envelope.Version)
	}
	if !strings.EqualFold(envelope.RecipientKeyID, k.KeyID()) {
		return nil, errors.New("secret envelope recipient does not match this worker")
	}
	ephemeralRaw, err := decodeField(envelope.EphemeralPublicKey, "ephemeral public key")
	if err != nil {
		return nil, err
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(ephemeralRaw)
	if err != nil {
		return nil, errors.New("invalid envelope ephemeral public key")
	}
	shared, err := k.private.ECDH(ephemeral)
	if err != nil {
		return nil, errors.New("derive envelope shared secret")
	}
	salt, err := decodeField(envelope.Salt, "salt")
	if err != nil {
		return nil, err
	}
	key, err := deriveKey(shared, salt, aad)
	if err != nil {
		return nil, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce, err := decodeField(envelope.Nonce, "nonce")
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, errors.New("invalid envelope nonce")
	}
	ciphertext, err := decodeField(envelope.Ciphertext, "ciphertext")
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, errors.New("secret envelope authentication failed")
	}
	return plaintext, nil
}

func deriveKey(shared, salt, aad []byte) ([]byte, error) {
	reader := hkdf.New(sha256.New, shared, salt, append([]byte("openlist-cluster-envelope-v1\x00"), aad...))
	key := make([]byte, keySize)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("derive envelope key: %w", err)
	}
	return key, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}
	return aead, nil
}

func decodeKey(value string) ([]byte, error) {
	raw, err := decodeField(value, "public key")
	if err != nil || len(raw) != keySize {
		return nil, errors.New("invalid X25519 public key")
	}
	return raw, nil
}

func decodeField(value, name string) ([]byte, error) {
	raw, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil || len(raw) == 0 {
		return nil, fmt.Errorf("invalid envelope %s", name)
	}
	return raw, nil
}
