package fs

import "testing"

func TestAliyunTransferSourceDriverNames(t *testing.T) {
	tests := []struct {
		driver string
		want   bool
	}{
		{driver: "Aliyundrive", want: true},
		{driver: "AliyundriveOpen", want: true},
		{driver: "AliyundriveShare", want: true},
		{driver: "Local", want: false},
		{driver: "139Yun", want: false},
		{driver: "Pan123", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			if got := isAliyunTransferSourceDriver(tt.driver); got != tt.want {
				t.Fatalf("isAliyunTransferSourceDriver(%q) = %v, want %v", tt.driver, got, tt.want)
			}
		})
	}
}
