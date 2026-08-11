package cluster

import (
	"fmt"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

func TestSubscriptionBatchIDForChunkIsUniqueAndStable(t *testing.T) {
	tasks := make([]subscription.ClusterMediaTask, 101)
	for i := range tasks {
		tasks[i] = subscription.ClusterMediaTask{IdempotencyKey: fmt.Sprintf("item-%03d", i)}
	}
	first := subscriptionBatchIDForChunk(tasks, 0, 100)
	second := subscriptionBatchIDForChunk(tasks, 100, 101)
	if first == second {
		t.Fatalf("chunk batch IDs collide: %q", first)
	}
	if first != subscriptionBatchIDForChunk(tasks, 0, 100) || second != subscriptionBatchIDForChunk(tasks, 100, 101) {
		t.Fatal("chunk batch IDs are not deterministic")
	}
	if len(first) > 63 || len(second) > 63 {
		t.Fatal("chunk batch ID exceeds storage limit")
	}
}
