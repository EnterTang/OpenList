package cluster

import "testing"

func TestMobile139MaxSingleUploadBytes(t *testing.T) {
	if got, want := mobile139MaxSingleUploadBytes("diamond"), int64(500<<30); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
	if got, want := mobile139MaxSingleUploadBytes("gold"), int64(20<<30); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}
