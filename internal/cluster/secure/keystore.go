package secure

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type persistedKey struct {
	Version    int    `json:"version"`
	PrivateKey string `json:"private_key"`
}

func LoadOrCreateKeyPair(filename string) (*KeyPair, error) {
	if filename == "" {
		return nil, errors.New("worker key filename is required")
	}
	raw, err := os.ReadFile(filename)
	if err == nil {
		var stored persistedKey
		if json.Unmarshal(raw, &stored) != nil || stored.Version != 1 {
			return nil, errors.New("invalid worker key file")
		}
		privateRaw, decodeErr := base64.RawStdEncoding.DecodeString(stored.PrivateKey)
		if decodeErr != nil {
			return nil, errors.New("invalid worker private key encoding")
		}
		return NewKeyPair(privateRaw)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read worker key file: %w", err)
	}
	keyPair, err := GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		return nil, fmt.Errorf("create worker key directory: %w", err)
	}
	encoded, err := json.Marshal(persistedKey{Version: 1, PrivateKey: base64.RawStdEncoding.EncodeToString(keyPair.PrivateBytes())})
	if err != nil {
		return nil, fmt.Errorf("marshal worker key: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(filename), ".cluster-key-*")
	if err != nil {
		return nil, fmt.Errorf("create worker key temp file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return nil, fmt.Errorf("protect worker key file: %w", err)
	}
	if _, err := temp.Write(encoded); err != nil {
		temp.Close()
		return nil, fmt.Errorf("write worker key file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return nil, fmt.Errorf("sync worker key file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return nil, fmt.Errorf("close worker key file: %w", err)
	}
	if err := os.Rename(tempName, filename); err != nil {
		return nil, fmt.Errorf("install worker key file: %w", err)
	}
	return keyPair, nil
}
