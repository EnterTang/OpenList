package _115_open

import (
	"context"
	"errors"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestOpen115RequestWaitHookReceivesRequestClass(t *testing.T) {
	wantErr := errors.New("request wait")
	var got RequestClass
	driver := &Open115{}
	driver.SetRequestWaitFunc(func(_ context.Context, class RequestClass) error {
		got = class
		return wantErr
	})

	err := driver.waitRequest(context.Background(), RequestDownloadURL)
	if !errors.Is(err, wantErr) {
		t.Fatalf("waitRequest() error = %v, want %v", err, wantErr)
	}
	if got != RequestDownloadURL {
		t.Fatalf("request class = %v, want %v", got, RequestDownloadURL)
	}
}

func TestOpen115InitUsesConfiguredRequestWaitBeforeNetwork(t *testing.T) {
	wantErr := errors.New("request wait")
	driver := &Open115{}
	driver.SetRequestWaitFunc(func(_ context.Context, class RequestClass) error {
		if class != RequestRESTAPI {
			t.Fatalf("request class = %v, want %v", class, RequestRESTAPI)
		}
		return wantErr
	})

	if err := driver.Init(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Init() error = %v, want %v", err, wantErr)
	}
}

func TestOpen115ListUsesFileListRequestWaitHook(t *testing.T) {
	wantErr := errors.New("request wait")
	driver := &Open115{}
	driver.SetRequestWaitFunc(func(_ context.Context, class RequestClass) error {
		if class != RequestFileList {
			t.Fatalf("request class = %v, want %v", class, RequestFileList)
		}
		return wantErr
	})

	_, err := driver.List(context.Background(), &Obj{Fid: "root"}, model.ListArgs{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("List() error = %v, want %v", err, wantErr)
	}
}

func TestOpen115GetDetailsUsesRESTRequestWaitHook(t *testing.T) {
	wantErr := errors.New("request wait")
	driver := &Open115{}
	driver.SetRequestWaitFunc(func(_ context.Context, class RequestClass) error {
		if class != RequestRESTAPI {
			t.Fatalf("request class = %v, want %v", class, RequestRESTAPI)
		}
		return wantErr
	})

	_, err := driver.GetDetails(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetDetails() error = %v, want %v", err, wantErr)
	}
}

func TestOpen115RefreshTokenHandlerCanDelegatePersistence(t *testing.T) {
	driver := &Open115{}
	var gotAccessToken, gotRefreshToken string
	driver.SetRefreshTokenHandler(func(accessToken, refreshToken string) {
		gotAccessToken = accessToken
		gotRefreshToken = refreshToken
	})

	driver.handleRefreshToken("access-new", "refresh-new")

	if driver.Addition.AccessToken != "access-new" || driver.Addition.RefreshToken != "refresh-new" {
		t.Fatalf("tokens = (%q, %q), want refreshed values", driver.Addition.AccessToken, driver.Addition.RefreshToken)
	}
	if gotAccessToken != "access-new" || gotRefreshToken != "refresh-new" {
		t.Fatalf("persistence callback = (%q, %q), want refreshed values", gotAccessToken, gotRefreshToken)
	}
}
