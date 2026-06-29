package category

import "testing"

const testRulesYAML = `movie:
  动作片:
    genre_ids: '28'
  剧情片:
    genre_ids: '18,28'
  外语电影:
    original_language: '!zh,!cn'
  未分类:
tv:
  国产剧:
    origin_country: 'CN'
    original_language: 'zh,cn'
  海外其他剧:
    origin_country: '!CN,!TW,!HK,!JP,!KR,!US,!GB'
  未分类:
`

func TestMatchGenreByOrder(t *testing.T) {
	got := Match(testRulesYAML, Metadata{MediaType: "movie", GenreIDs: []int{18, 28}, OriginalLanguage: "en"})

	if got != "动作片" {
		t.Fatalf("Match = %q, want first matching genre label", got)
	}
}

func TestMatchNegativeLanguage(t *testing.T) {
	got := Match(testRulesYAML, Metadata{MediaType: "movie", OriginalLanguage: "en"})
	if got != "外语电影" {
		t.Fatalf("Match = %q, want 外语电影", got)
	}

	got = Match(testRulesYAML, Metadata{MediaType: "movie", OriginalLanguage: "zh"})
	if got == "外语电影" {
		t.Fatal("negative language rule matched zh unexpectedly")
	}
}

func TestMatchFallbackEmptyRule(t *testing.T) {
	got := Match(testRulesYAML, Metadata{MediaType: "tv", OriginCountry: []string{"US"}, OriginalLanguage: "en"})

	if got != "未分类" {
		t.Fatalf("Match = %q, want fallback empty rule label", got)
	}
}

func TestInvalidYAMLFallsBackToDefaults(t *testing.T) {
	got := Match("movie:\n  动作片: [", Metadata{MediaType: "movie", GenreIDs: []int{16}})

	if got != "动画片" {
		t.Fatalf("Match invalid YAML = %q, want default 动画片", got)
	}
}
