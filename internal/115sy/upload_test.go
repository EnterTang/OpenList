package _115sy

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type uploadTestFile struct {
	*model.Object
	data   []byte
	exists model.Obj
}

func newUploadTestFile(data []byte) *uploadTestFile {
	hash := sha1.Sum(data)
	return &uploadTestFile{
		Object: &model.Object{
			ID:       "upload-test",
			Name:     "demo.bin",
			Size:     int64(len(data)),
			HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(hex.EncodeToString(hash[:]))),
		},
		data: data,
	}
}

func (f *uploadTestFile) Read(p []byte) (int, error) { return 0, io.EOF }

func (f *uploadTestFile) Close() error { return nil }

func (f *uploadTestFile) Add(io.Closer) {}

func (f *uploadTestFile) AddIfCloser(any) {}

func (f *uploadTestFile) GetMimetype() string { return "application/octet-stream" }

func (f *uploadTestFile) NeedStore() bool { return false }

func (f *uploadTestFile) IsForceStreamUpload() bool { return false }

func (f *uploadTestFile) GetExist() model.Obj { return f.exists }

func (f *uploadTestFile) SetExist(obj model.Obj) { f.exists = obj }

func (f *uploadTestFile) RangeRead(r http_range.Range) (io.Reader, error) {
	start := r.Start
	if start < 0 || start > int64(len(f.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	end := int64(len(f.data))
	if r.Length >= 0 && start+r.Length < end {
		end = start + r.Length
	}
	return io.NopCloser(bytes.NewReader(f.data[start:end])), nil
}

func (f *uploadTestFile) CacheFullAndWriter(_ *model.UpdateProgress, _ io.Writer) (model.File, error) {
	return nil, nil
}

func (f *uploadTestFile) GetFile() model.File { return nil }

func TestComputeUploadHashesUsesUppercaseFullAndPreSHA1(t *testing.T) {
	file := newUploadTestFile([]byte("0123456789abcdef"))
	hashes, err := ComputeUploadHashes(file, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hashes.Size != int64(len(file.data)) || hashes.PreHashLen != int64(len(file.data)) {
		t.Fatalf("hash metadata = %#v", hashes)
	}
	if hashes.SHA1 != hashes.PreSHA1 || hashes.SHA1 != strings.ToUpper(file.GetHash().GetHash(utils.SHA1)) {
		t.Fatalf("hashes = %#v, want matching uppercase SHA1", hashes)
	}
}

func TestUploadDigestRangeValidatesRequestedBytes(t *testing.T) {
	file := newUploadTestFile([]byte("0123456789"))
	wantBytes := []byte("2345")
	hash := sha1.Sum(wantBytes)
	got, err := UploadDigestRange(file, "2-5")
	if err != nil {
		t.Fatal(err)
	}
	if got != strings.ToUpper(hex.EncodeToString(hash[:])) {
		t.Fatalf("digest = %q, want %x", got, hash)
	}
	if _, err := UploadDigestRange(file, "5-2"); err == nil {
		t.Fatal("expected invalid range error")
	}
}

func TestUploadAvailableUsesDefaultAppVersionAndCachesResponse(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != EndpointUploadInfo {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("appversion"); got != DefaultAppVersion {
			t.Fatalf("appversion = %q, want %q", got, DefaultAppVersion)
		}
		if got := r.Header.Get("app"); got != string(ProfileAndroid) {
			t.Fatalf("app = %q, want android", got)
		}
		_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"user_id":42,"userkey":"upload-key","size_limit":1000,"upload_allowed":true}}`)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{AndroidBaseURL: server.URL, UploadBaseURL: server.URL, LimitRate: 1e6})
	first, err := client.UploadAvailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.UploadAvailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.UserID != 42 || first.UserKey != "upload-key" || second.UserID != first.UserID || calls != 1 {
		t.Fatalf("responses/calls = %#v/%#v/%d", first, second, calls)
	}
}

func TestUploadInitResponseRapidMatchContract(t *testing.T) {
	for status, want := range map[int]bool{1: false, 2: true} {
		matched, err := (UploadInitResponse{Status: status}).RapidMatched()
		if err != nil || matched != want {
			t.Fatalf("status %d = %v, %v; want %v, nil", status, matched, err, want)
		}
	}
	if _, err := (UploadInitResponse{Status: 9}).RapidMatched(); err == nil {
		t.Fatal("expected unexpected upload status error")
	}
}
