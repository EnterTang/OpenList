package _115sy

import (
	"context"
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
	"time"
)

type ErrorKind string

type RetryDisposition string

const (
	RetryDispositionNone            RetryDisposition = "none"
	RetryDispositionRetryAfter      RetryDisposition = "retry_after"
	RetryDispositionFallbackProfile RetryDisposition = "fallback_profile"
	RetryDispositionReauthorize     RetryDisposition = "reauthorize"
	RetryDispositionBlocked         RetryDisposition = "blocked"
	RetryDispositionTerminal        RetryDisposition = "terminal"
	RetryDispositionResultUnknown   RetryDisposition = "result_unknown"
)

type ResponseMeta struct {
	StatusCode  int
	ContentType string
	BodyKind    string
	BodyLength  int
	RetryAfter  time.Duration
	Endpoint    string
	Profile     Profile
}

const (
	KindNetwork  ErrorKind = "network"
	KindHTTP     ErrorKind = "http"
	KindBusiness ErrorKind = "business"
	KindAuth     ErrorKind = "auth"
	KindRisk     ErrorKind = "risk"
	KindParam    ErrorKind = "param"
	KindCancel   ErrorKind = "cancel"
)

type NetworkError struct {
	Kind     ErrorKind
	Method   string
	Endpoint string
	Profile  Profile
	Err      error
}

func (e *NetworkError) Error() string {
	return fmt.Sprintf("%s request %s %s failed: %s", e.Profile, e.Method, sanitizeEndpoint(e.Endpoint), sanitizeErrorCause(e.Err))
}

func (e *NetworkError) Unwrap() error {
	return e.Err
}

type HTTPError struct {
	Kind        ErrorKind
	StatusCode  int
	Endpoint    string
	Profile     Profile
	Meta        ResponseMeta
	Disposition RetryDisposition
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s request %s returned HTTP %d", e.Profile, sanitizeEndpoint(e.Endpoint), e.StatusCode)
}

type BusinessError struct {
	Kind     ErrorKind
	Errno    int
	Message  string
	Endpoint string
	Profile  Profile
}

func (e *BusinessError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		return fmt.Sprintf("%s request %s failed with errno %d", e.Profile, sanitizeEndpoint(e.Endpoint), e.Errno)
	}
	return fmt.Sprintf("%s request %s failed with errno %d: %s", e.Profile, sanitizeEndpoint(e.Endpoint), e.Errno, sanitizeMessage(message))
}

type CancelError struct {
	Kind     ErrorKind
	Endpoint string
	Profile  Profile
	Err      error
}

func (e *CancelError) Error() string {
	return fmt.Sprintf("%s request %s canceled: %s", e.Profile, sanitizeEndpoint(e.Endpoint), sanitizeErrorCause(e.Err))
}

func (e *CancelError) Unwrap() error {
	return e.Err
}

func classifyBusinessKind(errno int) ErrorKind {
	switch errno {
	case 40101017:
		return KindAuth
	default:
		return KindBusiness
	}
}

func wrapContextError(err error, endpoint string, profile Profile) error {
	if err == nil {
		return nil
	}
	if errorsIsContext(err) {
		return &CancelError{
			Kind:     KindCancel,
			Endpoint: endpoint,
			Profile:  profile,
			Err:      err,
		}
	}
	return err
}

func errorsIsContext(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func sanitizeEndpoint(endpoint string) string {
	if endpoint == "" {
		return "/"
	}
	parsed, err := neturl.Parse(endpoint)
	if err == nil {
		if parsed.Path != "" {
			return parsed.Path
		}
	}
	if idx := strings.Index(endpoint, "?"); idx >= 0 {
		return endpoint[:idx]
	}
	return endpoint
}

func sanitizeErrorCause(err error) string {
	if err == nil {
		return ""
	}
	var urlErr *neturl.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return sanitizeMessage(urlErr.Err.Error())
	}
	return sanitizeMessage(err.Error())
}

func sanitizeMessage(message string) string {
	redacted := message
	for _, key := range []string{"receive_code=", "security_code=", "Cookie:", "Cookie=", "UID=", "CID=", "SEID=", "KID="} {
		redacted = redactAllOccurrences(redacted, key)
	}
	return redacted
}

func redactAllOccurrences(message, key string) string {
	lowerMessage := strings.ToLower(message)
	lowerKey := strings.ToLower(key)
	var builder strings.Builder
	start := 0

	for {
		idx := strings.Index(lowerMessage[start:], lowerKey)
		if idx == -1 {
			builder.WriteString(message[start:])
			return builder.String()
		}

		matchStart := start + idx
		builder.WriteString(message[start:matchStart])
		builder.WriteString(message[matchStart : matchStart+len(key)])

		valueStart := matchStart + len(key)
		trimmedValueStart := valueStart
		for trimmedValueStart < len(message) {
			switch message[trimmedValueStart] {
			case ' ', '\t':
				builder.WriteByte(message[trimmedValueStart])
				trimmedValueStart++
			default:
				goto findValueEnd
			}
		}

	findValueEnd:
		valueEndOffset := strings.IndexAny(message[trimmedValueStart:], "&; \t\r\n\"")
		if valueEndOffset == -1 {
			builder.WriteString("[REDACTED]")
			return builder.String()
		}

		valueEnd := trimmedValueStart + valueEndOffset
		builder.WriteString("[REDACTED]")
		start = valueEnd
	}
}
