package guangyapan

import "testing"

func TestParseShareURL(t *testing.T) {
	t.Parallel()

	shareID, code, err := ParseShareURL("https://www.guangyapan.com/s/1908913489489252407_adyLT48EdLN_2AYh?code=abcd", "")
	if err != nil {
		t.Fatalf("ParseShareURL returned error: %v", err)
	}
	if shareID != "1908913489489252407_adyLT48EdLN_2AYh" {
		t.Fatalf("shareID = %q", shareID)
	}
	if code != "abcd" {
		t.Fatalf("code = %q, want abcd", code)
	}

	shareID, code, err = ParseShareURL("https://guangyapan.com/s/abc_def\n提取码：xy12", "")
	if err != nil {
		t.Fatalf("ParseShareURL text code error: %v", err)
	}
	if shareID != "abc_def" || code != "xy12" {
		t.Fatalf("got shareID=%q code=%q", shareID, code)
	}

	if _, _, err := ParseShareURL("https://example.com/s/nope", ""); err == nil {
		t.Fatal("expected error for unknown host")
	}
}

func TestNormalizePhoneAndDeviceID(t *testing.T) {
	t.Parallel()

	if got := normalizePhoneE164("13800138000"); got != "+86 13800138000" {
		t.Fatalf("normalizePhoneE164 = %q", got)
	}
	if got := normalizePhoneE164("+86 13800138000"); got != "+86 13800138000" {
		t.Fatalf("normalizePhoneE164 keep = %q", got)
	}
	if got := normalizeDeviceID("ABCDEF0123456789ABCDEF0123456789"); got != "abcdef0123456789abcdef0123456789" {
		t.Fatalf("normalizeDeviceID = %q", got)
	}
	if got := normalizeDeviceID("bad"); got != "" {
		t.Fatalf("normalizeDeviceID invalid = %q", got)
	}
}

func TestNormalizeOSSEndpointAndPartSize(t *testing.T) {
	t.Parallel()

	got := normalizeOSSEndpoint("https://bucket.oss-cn-hangzhou.aliyuncs.com", "bucket")
	if got != "https://oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("normalizeOSSEndpoint = %q", got)
	}
	if got := calcUploadPartSize(50 * 1024 * 1024); got != 1*1024*1024 {
		t.Fatalf("part size small = %d", got)
	}
	if got := calcUploadPartSize(20 * 1024 * 1024 * 1024); got != 4*1024*1024 {
		t.Fatalf("part size large = %d", got)
	}
}

func TestIsBizOKAndShareTaskStatus(t *testing.T) {
	t.Parallel()

	for _, code := range []any{0, "0", 200, "200", nil, "success"} {
		if !isBizOK(code) {
			t.Fatalf("isBizOK(%v) = false", code)
		}
	}
	if isBizOK("1001") {
		t.Fatal("isBizOK(1001) should be false")
	}
	if !shareTaskSucceeded("success", nil) || !shareTaskSucceeded(2, nil) || !shareTaskSucceeded(nil, 100) {
		t.Fatal("shareTaskSucceeded failed")
	}
	if !shareTaskFailed("failed") || !shareTaskFailed(3) {
		t.Fatal("shareTaskFailed failed")
	}
}
