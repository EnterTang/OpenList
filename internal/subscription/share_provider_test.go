package subscription

import "testing"

func TestDetectShareProvider(t *testing.T) {
	tests := []struct {
		raw      string
		provider ShareProviderName
	}{
		{raw: "https://pan.quark.cn/s/bc18e4ea5fb8", provider: ShareProviderQuark},
		{raw: "https://www.alipan.com/s/odeXVKsEKxr", provider: ShareProviderAliyunDrive},
		{raw: "https://www.aliyundrive.com/s/odeXVKsEKxr", provider: ShareProviderAliyunDrive},
		{raw: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", provider: ShareProviderPan123},
		{raw: "https://115cdn.com/s/swssal13zrk?password=t58d", provider: ShareProviderPan115},
		{raw: "https://115.com/s/swssal13zrk?password=t58d", provider: ShareProviderPan115},
	}
	for _, tt := range tests {
		t.Run(string(tt.provider), func(t *testing.T) {
			provider, ok := DetectShareProvider(tt.raw)
			if !ok {
				t.Fatalf("provider not detected for %s", tt.raw)
			}
			if provider != tt.provider {
				t.Fatalf("provider = %s, want %s", provider, tt.provider)
			}
		})
	}
}
