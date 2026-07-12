package subscription

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveClusterShareTempRootAppendsTaskNamespace(t *testing.T) {
	got, err := resolveClusterShareTempRoot("/ali/转存至移动", ".openlist-cluster/job-1/media-1")
	require.NoError(t, err)
	require.Equal(t, "/ali/转存至移动/.openlist-cluster/job-1/media-1", got)
}

func TestResolveClusterShareTempRootPreservesExplicitAbsoluteRoot(t *testing.T) {
	got, err := resolveClusterShareTempRoot("/ali/转存至移动", "/ali/custom")
	require.NoError(t, err)
	require.Equal(t, "/ali/custom", got)
}

func TestResolveClusterShareTempRootRejectsTraversal(t *testing.T) {
	_, err := resolveClusterShareTempRoot("/ali/转存至移动", "../../shared")
	require.Error(t, err)
}

func TestResolveClusterShareTempRootRequiresConfiguredBaseForNamespace(t *testing.T) {
	_, err := resolveClusterShareTempRoot("", ".openlist-cluster/job-1/media-1")
	require.Error(t, err)
}
