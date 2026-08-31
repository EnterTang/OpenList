//go:build windows

package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncDirectoryOnWindowsDoesNotRequireDirectorySync(t *testing.T) {
	if err := syncDirectory(t.TempDir()); err != nil {
		t.Fatalf("syncDirectory on Windows: %v", err)
	}
}

func TestNewLocalSourceCleanupTargetAcceptsWindowsPaths(t *testing.T) {
	target, err := NewLocalSourceCleanupTarget(`F:\downloads\staging`, `F:\downloads\staging\episode.mkv`)
	require.NoError(t, err)
	require.Equal(t, `F:\downloads\staging\episode.mkv`, target.LocalPath)
	require.Equal(t, `F:\downloads\staging`, target.OwnedRootPath)
	require.Equal(t, "episode.mkv", target.Name)
	require.True(t, target.ExactFile)
}
