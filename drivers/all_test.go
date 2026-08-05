package drivers

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func Test115CD2DriverIsRegistered(t *testing.T) {
	constructor, err := op.GetDriver("115 CD2")
	if err != nil {
		t.Fatalf("GetDriver(115 CD2) error = %v", err)
	}
	if got := constructor().Config().Name; got != "115 CD2" {
		t.Fatalf("registered driver name = %q, want 115 CD2", got)
	}
}

func Test115CD2TokensAreOptionalForDeviceLogin(t *testing.T) {
	info, ok := op.GetDriverInfoMap()["115 CD2"]
	if !ok {
		t.Fatal("115 CD2 driver info is not registered")
	}
	for _, item := range info.Additional {
		if (item.Name == "access_token" || item.Name == "refresh_token") && item.Required {
			t.Fatalf("%s must be optional so CD2 can use device authorization", item.Name)
		}
	}
}
