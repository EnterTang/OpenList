//go:build windows && amd64

package embeddedredis

import (
	"embed"
	"fmt"
)

//go:embed all:assets/generated
var embeddedPayloadFiles embed.FS

func EmbeddedPayload() ([]byte, error) {
	payload, err := embeddedPayloadFiles.ReadFile("assets/generated/" + PayloadFilename)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrPayloadUnavailable, PayloadFilename)
	}
	return payload, nil
}
