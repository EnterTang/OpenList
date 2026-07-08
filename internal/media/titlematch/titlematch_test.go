package titlematch

import "testing"

func TestNormalizeMediaTitleRemovesEnglishAndChineseNoise(t *testing.T) {
	got := NormalizeMediaTitle("美剧.诊疗中 第三季 Shrinking Season 3.2026.2160p.WEB-DL.DDP5.1.Atmos 内封字幕")
	want := "诊疗中 Shrinking"
	if got != want {
		t.Fatalf("NormalizeMediaTitle = %q, want %q", got, want)
	}
}

func TestNormalizeMediaTitleKeepsPureYearTitles(t *testing.T) {
	cases := map[string]string{
		"1917":         "1917",
		"1923（黄石前传）": "1923",
	}
	for input, want := range cases {
		if got := NormalizeMediaTitle(input); got != want {
			t.Fatalf("NormalizeMediaTitle(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBuildMediaQueryCandidatesIncludesBilingualAndPrefixStrippedForms(t *testing.T) {
	got := BuildMediaQueryCandidates("韩剧《亲爱的X》 Dear X 第1季 1080p")
	wants := []string{"亲爱的 X Dear X", "亲爱的 X", "Dear X"}
	for _, want := range wants {
		found := false
		for _, item := range got {
			if item == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("BuildMediaQueryCandidates missing %q in %#v", want, got)
		}
	}
}

func TestTitlesCompatibleRejectsGenericOnlyTitles(t *testing.T) {
	if TitlesCompatible("第三季", "权力的游戏 第三季") {
		t.Fatal("TitlesCompatible returned true for generic-only query")
	}
}

func TestTitlesCompatibleAcceptsNoisyBilingualEquivalentTitles(t *testing.T) {
	if !TitlesCompatible("Rain Man 1988 蓝光原盘", "雨人 Rain Man (1988)") {
		t.Fatal("TitlesCompatible returned false for equivalent bilingual titles")
	}
}

func TestTitlesCompatibleAcceptsSingleChineseCoreTitleAgainstBilingualName(t *testing.T) {
	if !TitlesCompatible("雨人", "雨人 Rain Man (1988)") {
		t.Fatal("TitlesCompatible returned false for single Chinese core title")
	}
}

func TestTitlesCompatibleRejectsDocumentaryStyleSuffixTitles(t *testing.T) {
	if TitlesCompatible("千与千寻", "千与千寻诞生秘话") {
		t.Fatal("TitlesCompatible returned true for documentary-style suffix title")
	}
}

func TestScoreTitleMatchPrefersExactEquivalentOverWeakSubstring(t *testing.T) {
	good := ScoreTitleMatch("Shrinking", "诊疗中 Shrinking")
	bad := ScoreTitleMatch("Shrinking", "The Making of Shrinking")
	if good <= bad {
		t.Fatalf("good score = %d, bad score = %d, want good > bad", good, bad)
	}
}
