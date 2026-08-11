package coordinator

import "testing"

func TestShouldRetryMediaTransferClassifiesProviderFailures(t *testing.T) {
	for _, code := range []string{"share_save_rate_limited", "share_save_gateway_response", "share_save_transient", "source_unexpected_eof", "source_link_expired", "network_timeout"} {
		if !shouldRetryMediaTransfer(code, 0) {
			t.Fatalf("error code %q should be retryable", code)
		}
	}
	for _, code := range []string{"share_save_credentials_invalid", "share_save_method_not_allowed", "share_save_source_invalid"} {
		if shouldRetryMediaTransfer(code, 0) {
			t.Fatalf("error code %q should not be retryable", code)
		}
	}
}
