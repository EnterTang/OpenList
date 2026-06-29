package _139

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestPersonalNewUploadPartInfosCover500GiBWithinLimit(t *testing.T) {
	const size = 500 * utils.GB

	d := &Yun139{}
	partInfos := d.buildPersonalUploadPartInfos(size)

	if len(partInfos) != personalUploadPartInfoLimit {
		t.Fatalf("part count = %d, want %d", len(partInfos), personalUploadPartInfoLimit)
	}

	var total int64
	for i, partInfo := range partInfos {
		if partInfo.PartNumber != int64(i+1) {
			t.Fatalf("part %d number = %d, want %d", i, partInfo.PartNumber, i+1)
		}
		if partInfo.ParallelHashCtx.PartOffset != total {
			t.Fatalf("part %d offset = %d, want %d", i, partInfo.ParallelHashCtx.PartOffset, total)
		}
		total += partInfo.PartSize
	}
	if total != size {
		t.Fatalf("total part size = %d, want %d", total, size)
	}
}

func TestPersonalUploadHeadersUseMacCloudClientIdentity(t *testing.T) {
	d := &Yun139{Addition: Addition{UserDomainID: "device-from-config"}}

	headers := d.personalUploadHeaders()
	clientInfo := headers["X-Yun-Client-Info"]

	if headers["User-Agent"] != personalUploadUserAgent {
		t.Fatalf("User-Agent = %q, want %q", headers["User-Agent"], personalUploadUserAgent)
	}
	if !strings.HasPrefix(clientInfo, "||13|12.27.0|PC|QkYtMjAyMDAzMTAxNjQ3|") {
		t.Fatalf("client info = %q, want mac client type 13 prefix", clientInfo)
	}
	if !strings.Contains(clientInfo, "|device-from-config||macOS 13.6|1978X1127|") {
		t.Fatalf("client info = %q, want configured device id", clientInfo)
	}
	if headers["x-DeviceInfo"] != clientInfo {
		t.Fatalf("x-DeviceInfo = %q, want client info", headers["x-DeviceInfo"])
	}
	if headers["x-yun-device-id"] != "device-from-config" {
		t.Fatalf("x-yun-device-id = %q, want configured device id", headers["x-yun-device-id"])
	}
	if headers["X-Yun-App-Channel"] != "10301000" {
		t.Fatalf("X-Yun-App-Channel = %q, want 10301000", headers["X-Yun-App-Channel"])
	}
}

func TestPersonalUploadNeedsPartUploadOnlyWhenPartURLsReturned(t *testing.T) {
	var rapidHit PersonalUploadResp
	rapidHit.Data.PartInfos = []PersonalPartInfo{}
	if personalUploadNeedsPartUpload(rapidHit) {
		t.Fatal("empty partInfos should be treated as rapid upload hit")
	}

	var needUpload PersonalUploadResp
	needUpload.Data.PartInfos = []PersonalPartInfo{{PartNumber: 1, UploadUrl: "https://upload.example"}}
	if !personalUploadNeedsPartUpload(needUpload) {
		t.Fatal("partInfos with upload URL should require part upload")
	}
}

func TestUploadPersonalPartsFallsBackToCdnUploadURL(t *testing.T) {
	body := []byte("upload-body")
	var sawUpload bool
	oldHTTPClient := base.HttpClient
	base.HttpClient = http.DefaultClient
	defer func() {
		base.HttpClient = oldHTTPClient
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawUpload = true
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.Header.Get("User-Agent") != personalUploadUserAgent {
			t.Fatalf("User-Agent = %q, want %q", r.Header.Get("User-Agent"), personalUploadUserAgent)
		}
		gotBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !bytes.Equal(gotBody, body) {
			t.Fatalf("body = %q, want %q", gotBody, body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := &Yun139{}
	err := d.uploadPersonalParts(
		context.Background(),
		[]PartInfo{{PartNumber: 1, PartSize: int64(len(body))}},
		[]PersonalPartInfo{{PartNumber: 1, CdnUploadUrl: server.URL}},
		driver.NewLimitedUploadStream(context.Background(), bytes.NewReader(body)),
		driver.NewProgress(int64(len(body)), func(float64) {}),
	)
	if err != nil {
		t.Fatalf("uploadPersonalParts returned error: %v", err)
	}
	if !sawUpload {
		t.Fatal("expected upload request")
	}
}
