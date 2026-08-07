package subscription

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestNormalizeExternalSubscriptionAcceptsHDHiveSource(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	_, sub, _, _, _, err := normalizeExternalSubscriptionCreateRequest(ExternalSubscriptionCreateRequest{
		Name:       "HDHive source",
		MediaType:  "tv",
		TMDBID:     1399,
		SourceType: model.SubscriptionSourceHDHive,
	})
	if err != nil {
		t.Fatalf("normalize HDHive subscription: %v", err)
	}
	if sub == nil || sub.SourceType != model.SubscriptionSourceHDHive {
		t.Fatalf("subscription = %#v", sub)
	}
	var source model.SubscriptionHDHiveSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode source config: %v", err)
	}
	if source.CloudType != "all" || source.Limit <= 0 {
		t.Fatalf("source config = %#v", source)
	}
}
