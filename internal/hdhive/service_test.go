package hdhive

import (
	"context"
	"testing"
)

type fakeClient struct {
	statusCalls int
	shareCalls  int
	unlockCalls int
	details     ResourceDetails
	unlock      UnlockResult
}

func (f *fakeClient) Status(context.Context) (Status, error) {
	f.statusCalls++
	return Status{Authorized: true}, nil
}

func (f *fakeClient) Share(context.Context, string) (ResourceDetails, error) {
	f.shareCalls++
	return f.details, nil
}

func (f *fakeClient) Unlock(context.Context, string) (UnlockResult, error) {
	f.unlockCalls++
	return f.unlock, nil
}

func TestServiceCachesUnlockedLinkAndAvoidsSecondPaidAction(t *testing.T) {
	client := &fakeClient{
		details: ResourceDetails{UnlockPoints: intPtr(1)},
		unlock:  UnlockResult{FullURL: "https://115cdn.com/s/unlocked?password=abcd"},
	}
	service := NewService(client)
	ref := ResourceRef{SiteID: "115", Slug: "054da9afa2204d33a11831e58776d1e4"}

	first, err := service.Resolve(context.Background(), []ResourceRef{ref}, Config{Enabled: true})
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	second, err := service.Resolve(context.Background(), []ResourceRef{ref}, Config{Enabled: true})
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}

	if len(first.CloudLinks) != 1 || first.CloudLinks[0] != client.unlock.FullURL {
		t.Fatalf("first result = %#v", first)
	}
	if len(second.CloudLinks) != 1 || !second.Items[0].FromCache {
		t.Fatalf("second result = %#v", second)
	}
	if client.statusCalls != 1 || client.shareCalls != 1 || client.unlockCalls != 1 {
		t.Fatalf("calls = status:%d share:%d unlock:%d, want one paid resolution", client.statusCalls, client.shareCalls, client.unlockCalls)
	}
}

func TestServiceUsesAlreadyUnlockedLinkWithoutUnlocking(t *testing.T) {
	client := &fakeClient{details: ResourceDetails{
		FullURL:      "https://115cdn.com/s/owned?password=abcd",
		AccessCode:   "abcd",
		UnlockPoints: intPtr(1),
	}}
	service := NewService(client)

	result, err := service.Resolve(context.Background(), []ResourceRef{{SiteID: "115", Slug: "22c7835aacad4e3f9fee349d2d803cb1"}}, Config{Enabled: true})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(result.CloudLinks) != 1 || client.unlockCalls != 0 {
		t.Fatalf("result = %#v, unlock calls = %d", result, client.unlockCalls)
	}
}

func TestServiceSkipsUnlockAbovePointLimit(t *testing.T) {
	client := &fakeClient{details: ResourceDetails{UnlockPoints: intPtr(3)}}
	service := NewService(client)

	result, err := service.Resolve(context.Background(), []ResourceRef{{SiteID: "115", Slug: "22c7835aacad4e3f9fee349d2d803cb1"}}, Config{Enabled: true, MaxUnlockPoints: 2})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(result.Skipped) != 1 || result.Skipped[0].Reason != "unlock_points_exceeded" || client.unlockCalls != 0 {
		t.Fatalf("result = %#v, unlock calls = %d", result, client.unlockCalls)
	}
}

func intPtr(value int) *int { return &value }
