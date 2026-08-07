package hdhive

import "testing"

func TestExtractResourceRefsFromTextAndTelegramLinks(t *testing.T) {
	refs := ExtractResourceRefs(
		`标题 https://hdhive.com/resource/115/054da9afa2204d33a11831e58776d1e4?from=telegram`,
		[]string{
			"https://www.hdhive.com/resource/115/054da9af-a220-4d33-a118-31e58776d1e4",
			"https://example.com/not-hdhive",
		},
		"189",
	)

	if len(refs) != 1 {
		t.Fatalf("refs = %#v, want one unique resource", refs)
	}
	if refs[0].SiteID != "115" || refs[0].Slug != "054da9afa2204d33a11831e58776d1e4" {
		t.Fatalf("ref = %#v, want site 115 and normalized slug", refs[0])
	}
}

func TestResourceRefFromURLUsesDefaultSiteID(t *testing.T) {
	ref, ok := ResourceRefFromURL("https://hdhive.com/resource/22c7835aacad4e3f9fee349d2d803cb1", "189")
	if !ok {
		t.Fatal("ResourceRefFromURL returned false")
	}
	if ref.SiteID != "189" || ref.Slug != "22c7835aacad4e3f9fee349d2d803cb1" {
		t.Fatalf("ref = %#v, want default site and slug", ref)
	}
}
