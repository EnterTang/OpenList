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

func TestTorrentBoundPayloadAcceptsWindowsContentPath(t *testing.T) {
	event := BridgeEvent{
		EventID:   "ae13a7ac-99bc-4fa3-8417-3c9f466af1cf",
		Type:      EventTorrentBound,
		RequestID: "83264f37-1cd6-4c49-9d6e-456f5e463d0c",
		Torrent: &TorrentBoundPayload{
			Downloader:  "qb-win",
			TorrentHash: strings.Repeat("a", 40),
			ContentPath: `F:\downloads\Rounders`,
		},
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("Windows torrent.bound content path: %v", err)
	}
}

func TestBridgePayloadRejectsForbiddenSecretAndLocalPathFields(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"request_id":"r","site_cookie":"secret"}`),
		[]byte(`{"nested":{"qb_password":"secret"}}`),
		[]byte(`{"items":[{"qb_url":"http://qb"}]}`),
		[]byte(`{"torrent":{"local_path":"/downloads/file.mkv"}}`),
		[]byte(`{"torrent":{"enclosure":"https://pt.example/download?passkey=secret"}}`),
	} {
		if err := validateNoForbiddenBridgeFields(body); err == nil || !strings.Contains(err.Error(), "forbidden") {
			t.Fatalf("forbidden payload %s error = %v", body, err)
		}
	}
}

func TestDownloadIntentRequiresOpaqueResourceReference(t *testing.T) {
	request := DownloadIntentRequest{
		RequestID: "request-opaque", Media: MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{Enclosure: "https://pt.example/download?passkey=secret"},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "resource_ref") {
		t.Fatalf("enclosure-only intent error = %v", err)
	}
}

func TestDownloadIntentRejectsDirectURLDisguisedAsResourceReference(t *testing.T) {
	request := DownloadIntentRequest{
		RequestID:        "request-direct-url",
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{ResourceRef: "https://pt.example/download?id=1&passkey=secret"},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "opaque") {
		t.Fatalf("direct resource URL error = %v", err)
	}
}
