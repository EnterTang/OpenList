package cluster

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionDirectDownloadUsesSeparateCapability(t *testing.T) {
	capabilities := subscriptionMediaRequiredCapabilities(subscription.ClusterMediaTask{DeliveryMode: model.SubscriptionDeliveryModeDirectDownload})
	require.Contains(t, capabilities, "share.download")
	require.Contains(t, capabilities, "mobile.upload")
	transferCapabilities := subscriptionMediaRequiredCapabilities(subscription.ClusterMediaTask{DeliveryMode: model.SubscriptionDeliveryModeTransfer})
	require.Contains(t, transferCapabilities, "share.save")
	require.NotContains(t, transferCapabilities, "share.download")
}

func TestSubscriptionDirectDownloadDoesNotRequireShareSaveAccount(t *testing.T) {
	task := subscription.ClusterMediaTask{ShareProvider: "pan123", DeliveryMode: model.SubscriptionDeliveryModeDirectDownload}
	context := subscriptionMediaTaskContext(task, "mobile-primary")
	if context.StagingTarget.NeedShareSave {
		t.Fatal("direct download should not require cloud-side share.save")
	}
	if !context.StagingTarget.NeedShareDownload {
		t.Fatal("direct download should require a source account with share download support")
	}
}
