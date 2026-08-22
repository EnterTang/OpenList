package moviepilotbridge

import (
	"strings"
	"testing"
)

func TestTorrentBoundPayloadRequiresResolvedDownloaderAndHash(t *testing.T) {
	event := BridgeEvent{
		EventID:   "2b7d5ff9-6c18-4f53-b7b7-3522e6f58ad7",
		Type:      EventTorrentBound,
		RequestID: "9e63fcd0-dbc0-4cb5-bac4-1b3d727239c5",
		Torrent: &TorrentBoundPayload{
			Downloader:  "qb-hk",
			TorrentHash: strings.Repeat("a", 40),
			ContentPath: "/downloads/a",
		},
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid torrent.bound event: %v", err)
	}
	event.Torrent.Downloader = ""
	if err := event.Validate(); err == nil || err.Error() != "torrent.bound downloader is required" {
		t.Fatalf("empty downloader error = %v", err)
	}
}
