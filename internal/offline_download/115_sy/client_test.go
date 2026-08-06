package _115_sy

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/offline_download/tool"
)

func TestCloud115SYToolContract(t *testing.T) {
	client := &Cloud115SY{}
	if client.Name() != "115 SY" {
		t.Fatalf("Name() = %q", client.Name())
	}
	if err := client.Run(&tool.DownloadTask{}); err == nil {
		t.Fatal("Run() should not be supported")
	}
}
