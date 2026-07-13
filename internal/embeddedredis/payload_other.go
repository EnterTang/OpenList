//go:build !windows

package embeddedredis

import "fmt"

func EmbeddedPayload() ([]byte, error) {
	return nil, fmt.Errorf("%w: supported only on Windows", ErrPayloadUnavailable)
}
