package _115_sy

import (
	"reflect"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func Test115SYMetadata(t *testing.T) {
	if config.Name != "115 SY" {
		t.Fatalf("config name = %q, want 115 SY", config.Name)
	}
	if config.DefaultRoot != "0" {
		t.Fatalf("config default root = %q, want 0", config.DefaultRoot)
	}
	if config.LinkCacheMode != driver.LinkCacheUA {
		t.Fatalf("link cache mode = %d, want user-agent-aware caching", config.LinkCacheMode)
	}
}

func Test115SYAddition(t *testing.T) {
	addition := Addition{}
	addition.Cookie = "UID=uid;CID=0;SEID=seid"
	addition.QRCodeToken = "qr-token"
	addition.QRCodeSource = "android"
	addition.RootFolderID = "123"
	addition.PageSize = 100
	addition.PageCooldown = "250ms"
	addition.LimitRate = 2
	addition.UserAgent = "ua"
	addition.AppVersion = "36.2.28"
	addition.ShareCIDs = "movies:1,shows:2"
	addition.OfflineCID = "cid-offline"
	addition.AutomationInterval = "15m"
	addition.MembershipTier = "vip"

	if addition.Cookie == "" || addition.RootFolderID != "123" || addition.PageSize != 100 {
		t.Fatalf("addition fields were not retained: %#v", addition)
	}

	fields := map[string]struct {
		wantType    reflect.Type
		wantDefault string
	}{
		"Cookie":             {wantType: reflect.TypeOf(""), wantDefault: ""},
		"QRCodeToken":        {wantType: reflect.TypeOf(""), wantDefault: ""},
		"QRCodeSource":       {wantType: reflect.TypeOf(""), wantDefault: "android"},
		"PageSize":           {wantType: reflect.TypeOf(int64(0)), wantDefault: "200"},
		"PageCooldown":       {wantType: reflect.TypeOf(""), wantDefault: "250ms"},
		"LimitRate":          {wantType: reflect.TypeOf(float64(0)), wantDefault: "1"},
		"UserAgent":          {wantType: reflect.TypeOf(""), wantDefault: ""},
		"AppVersion":         {wantType: reflect.TypeOf(""), wantDefault: "36.2.28"},
		"ShareCIDs":          {wantType: reflect.TypeOf(""), wantDefault: ""},
		"OfflineCID":         {wantType: reflect.TypeOf(""), wantDefault: ""},
		"AutomationInterval": {wantType: reflect.TypeOf(""), wantDefault: ""},
		"MembershipTier":     {wantType: reflect.TypeOf(""), wantDefault: "unknown"},
	}

	additionType := reflect.TypeOf(addition)
	for name, want := range fields {
		field, ok := additionType.FieldByName(name)
		if !ok {
			t.Fatalf("Addition is missing %s", name)
		}
		if field.Type != want.wantType {
			t.Fatalf("%s type = %v, want %v", name, field.Type, want.wantType)
		}
		if got := field.Tag.Get("default"); got != want.wantDefault {
			t.Fatalf("%s default tag = %q, want %q", name, got, want.wantDefault)
		}
	}
}

func Test115SYObjImplementsContracts(t *testing.T) {
	obj := &Obj{
		FID:       "file-id",
		CID:       "dir-id",
		Name:      "demo.mkv",
		Directory: false,
		Size:      4096,
		SHA1:      "sha1-value",
		PickCode:  "pick-code",
		ParentCID: "parent-id",
		CreatedAt: 1710000000,
		UpdatedAt: 1710000300,
		Thumbnail: "https://example.com/thumb.jpg",
	}

	if got := obj.GetID(); got != "file-id" {
		t.Fatalf("GetID() = %q, want file-id", got)
	}
	if got := obj.GetName(); got != "demo.mkv" {
		t.Fatalf("GetName() = %q, want demo.mkv", got)
	}
	if got := obj.GetSize(); got != 4096 {
		t.Fatalf("GetSize() = %d, want 4096", got)
	}
	if obj.IsDir() {
		t.Fatal("IsDir() = true, want false")
	}
	if got := obj.GetPath(); got != "" {
		t.Fatalf("GetPath() = %q, want empty string", got)
	}
	if got := obj.Thumb(); got != "https://example.com/thumb.jpg" {
		t.Fatalf("Thumb() = %q, want thumbnail URL", got)
	}
	if got := obj.GetHash().GetHash(utils.SHA1); got != "sha1-value" {
		t.Fatalf("GetHash().GetHash(SHA1) = %q, want sha1-value", got)
	}
	if got := obj.CreateTime(); !got.Equal(time.Unix(1710000000, 0)) {
		t.Fatalf("CreateTime() = %v, want %v", got, time.Unix(1710000000, 0))
	}
	if got := obj.ModTime(); !got.Equal(time.Unix(1710000300, 0)) {
		t.Fatalf("ModTime() = %v, want %v", got, time.Unix(1710000300, 0))
	}

	var _ model.Obj = obj
	var _ model.Thumb = obj
}
