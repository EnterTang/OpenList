package subscription

import "testing"

func TestParseShareURLExamples(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		provider ShareProviderName
		shareID  string
		passcode string
	}{
		{name: "quark", raw: "https://pan.quark.cn/s/bc18e4ea5fb8", provider: ShareProviderQuark, shareID: "bc18e4ea5fb8"},
		{name: "aliyun", raw: "https://www.alipan.com/s/odeXVKsEKxr", provider: ShareProviderAliyunDrive, shareID: "odeXVKsEKxr"},
		{name: "123", raw: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#", provider: ShareProviderPan123, shareID: "7Tx1jv-pVu7v", passcode: "xoxo"},
		{name: "123 fastlink", raw: "123FSLinkV2$a3531a60736740a152e931a6ecee9bfb#500797103#食神·百厨大战.2025.S02E05.第5集.1080p.WEB-DL.SDR.H.264.25fps.AAC 2.0 {tmdb-297509}.mp4", provider: ShareProviderPan123, shareID: "a3531a60736740a152e931a6ecee9bfb"},
		{name: "115", raw: "https://115cdn.com/s/swssal13zrk?password=t58d", provider: ShareProviderPan115, shareID: "swssal13zrk", passcode: "t58d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseShareURL(tt.raw)
			if err != nil {
				t.Fatalf("parse share URL: %v", err)
			}
			if ref.Provider != tt.provider || ref.ShareID != tt.shareID || ref.Passcode != tt.passcode {
				t.Fatalf("ref = %#v, want provider=%s shareID=%s passcode=%s", ref, tt.provider, tt.shareID, tt.passcode)
			}
		})
	}
}

func TestParseShareURLRejectsInvalidPan123FastLink(t *testing.T) {
	if _, err := ParseShareURL("123FSLinkV2$badetag#500797103#video.mp4"); err == nil {
		t.Fatal("expected invalid fastlink error")
	}
}

func TestParseShareURLRejectsUnknownHost(t *testing.T) {
	if _, err := ParseShareURL("https://example.com/s/not-pan"); err == nil {
		t.Fatal("expected unknown share URL error")
	}
}

func TestParseShareURLUsesCommaPasscodeFallback(t *testing.T) {
	ref, err := ParseShareURL("https://pan.quark.cn/s/bc18e4ea5fb8,xoxo")
	if err != nil {
		t.Fatalf("parse share URL: %v", err)
	}
	if ref.Provider != ShareProviderQuark || ref.ShareID != "bc18e4ea5fb8" || ref.Passcode != "xoxo" {
		t.Fatalf("ref = %#v, want quark share with comma passcode", ref)
	}
}
