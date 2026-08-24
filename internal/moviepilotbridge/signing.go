package moviepilotbridge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	SignatureVersionV1 = "v1"

	HeaderVersion    = "X-OpenList-Bridge-Version"
	HeaderInstanceID = "X-OpenList-Bridge-Instance"
	HeaderTimestamp  = "X-OpenList-Bridge-Timestamp"
	HeaderNonce      = "X-OpenList-Bridge-Nonce"
	HeaderSignature  = "X-OpenList-Bridge-Signature"

	DefaultMaxClockSkew  = 5 * time.Minute
	BridgeNonceRetention = 2 * DefaultMaxClockSkew
)

// SignRequest describes the exact HTTP request covered by the Bridge MAC.
// Path is the request URI path including the query string when one exists.
type SignRequest struct {
	Version    string
	InstanceID string
	Method     string
	Path       string
	Timestamp  time.Time
	Nonce      string
	Body       []byte
}

func (r SignRequest) Canonical() ([]byte, error) {
	version := strings.TrimSpace(r.Version)
	instanceID := strings.TrimSpace(r.InstanceID)
	method := strings.ToUpper(strings.TrimSpace(r.Method))
	path := strings.TrimSpace(r.Path)
	nonce := strings.TrimSpace(r.Nonce)
	if version == "" || instanceID == "" || method == "" || path == "" || nonce == "" || r.Timestamp.IsZero() {
		return nil, errors.New("bridge signing fields are incomplete")
	}
	if !strings.HasPrefix(path, "/") {
		return nil, errors.New("bridge signing path must be absolute")
	}
	bodyHash := sha256.Sum256(r.Body)
	canonical := strings.Join([]string{
		version,
		instanceID,
		method,
		path,
		strconv.FormatInt(r.Timestamp.Unix(), 10),
		nonce,
		hex.EncodeToString(bodyHash[:]),
	}, "\n")
	return []byte(canonical), nil
}

func (r SignRequest) Signature(key []byte) (string, error) {
	if len(key) == 0 {
		return "", errors.New("bridge signing key is empty")
	}
	canonical, err := r.Canonical()
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (r SignRequest) Headers(key []byte) (http.Header, error) {
	signature, err := r.Signature(key)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set(HeaderVersion, strings.TrimSpace(r.Version))
	headers.Set(HeaderInstanceID, strings.TrimSpace(r.InstanceID))
	headers.Set(HeaderTimestamp, strconv.FormatInt(r.Timestamp.Unix(), 10))
	headers.Set(HeaderNonce, strings.TrimSpace(r.Nonce))
	headers.Set(HeaderSignature, signature)
	return headers, nil
}

// VerifyRequest is the input used when checking an inbound Bridge request.
type VerifyRequest struct {
	Headers http.Header
	Method  string
	Path    string
	Body    []byte
	Now     time.Time
}

func parseSignedRequest(request VerifyRequest) (SignRequest, error) {
	if request.Headers == nil {
		return SignRequest{}, errors.New("bridge signature headers are required")
	}
	version := strings.TrimSpace(request.Headers.Get(HeaderVersion))
	if version != SignatureVersionV1 {
		return SignRequest{}, fmt.Errorf("unsupported bridge signature version %q", version)
	}
	timestampRaw := strings.TrimSpace(request.Headers.Get(HeaderTimestamp))
	timestamp, err := strconv.ParseInt(timestampRaw, 10, 64)
	if err != nil {
		return SignRequest{}, errors.New("bridge timestamp is invalid")
	}
	return SignRequest{
		Version:    version,
		InstanceID: strings.TrimSpace(request.Headers.Get(HeaderInstanceID)),
		Method:     request.Method,
		Path:       request.Path,
		Timestamp:  time.Unix(timestamp, 0).UTC(),
		Nonce:      strings.TrimSpace(request.Headers.Get(HeaderNonce)),
		Body:       request.Body,
	}, nil
}

func verifySignature(request VerifyRequest, key []byte) (SignRequest, error) {
	signed, err := parseSignedRequest(request)
	if err != nil {
		return SignRequest{}, err
	}
	expected, err := signed.Signature(key)
	if err != nil {
		return SignRequest{}, err
	}
	provided := strings.TrimSpace(request.Headers.Get(HeaderSignature))
	if len(provided) != len(expected) || !hmac.Equal([]byte(provided), []byte(expected)) {
		return SignRequest{}, errors.New("invalid bridge signature")
	}
	return signed, nil
}
