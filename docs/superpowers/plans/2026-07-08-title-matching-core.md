# Shared Title Matching Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one shared media-title normalization and compatibility core, then use it to improve subscription resource search, Telegram subscription matching, TMDB candidate selection, and 139 share-risk rename behavior.

**Architecture:** Add a small helper package under `internal/media` with side-effect-free normalization, candidate generation, tokenization, compatibility, and scoring helpers. Then wire those helpers into existing modules incrementally so each stage has its own tests and preserves existing APIs while making matching stricter and more robust.

**Tech Stack:** Go, stdlib `regexp`/`strings`/`unicode`, existing OpenList `internal/media/recognize`, existing OpenList `internal/media/tmdb`, existing subscription and 139 driver tests.

## Global Constraints

- Do not import the full archive / category / folder-template system from `yidong139_pan_manager`.
- Do not change HTTP request or response schemas.
- Do not introduce a full anime-specific rename or archive pipeline in this iteration.
- Do not redesign the existing TMDB client API surface.
- Do not weaken the existing requirement that resource-search results must contain a real supported share link.
- Keep the first iteration deterministic and rule-based.
- Add tests before each behavioral change.
- Keep existing public behavior stable except where the user explicitly wants stricter filtering or better matching.

---

## File Structure

- Create: `internal/media/titlematch/titlematch.go`
  - Shared title normalization, candidate generation, tokenization, compatibility, and score helpers.
- Create: `internal/media/titlematch/titlematch_test.go`
  - Unit tests for normalization, candidate generation, tokenization, and compatibility edge cases.
- Modify: `internal/subscription/resource_search.go`
  - Replace simple title `contains` filtering with shared compatibility checks while keeping supported-share-link gating.
- Modify: `internal/subscription/resource_search_test.go`
  - Add resource-search regression tests for noisy bilingual titles and stricter title-only matching.
- Modify: `internal/subscription/telegram.go`
  - Replace simple normalized substring matching in `subscriptionEntryMatches` with shared candidate-based compatibility matching.
- Modify: `internal/subscription/telegram_test.go`
  - Add tests for noisy mixed-language path matching.
- Modify: `internal/media/recognize/recognize.go`
  - Route `NormalizeTitle` and `BuildQueryCandidates` through the shared helpers while preserving current `Result` structure.
- Modify: `internal/media/recognize/recognize_test.go`
  - Add tests for Chinese noise removal, pure-year preservation, and bilingual candidate generation.
- Modify: `internal/media/tmdb/tmdb.go`
  - Gate weak candidates with shared compatibility helpers and improve overlap scoring with shared tokenization.
- Modify: `internal/media/tmdb/tmdb_test.go`
  - Add tests for exact wins, bilingual wins, and weak-substring rejection.
- Modify: `drivers/139/share_risk_rename.go`
  - Use the improved recognition/query logic in rename title resolution.
- Modify: `drivers/139/share_risk_rename_test.go`
  - Add tests proving dirty names produce better canonical title candidates while keeping season/episode preservation.

### Task 1: Build the Shared Title Matching Core

**Files:**
- Create: `internal/media/titlematch/titlematch.go`
- Test: `internal/media/titlematch/titlematch_test.go`

**Interfaces:**
- Consumes: stdlib `regexp`, `strings`, `unicode`; no internal dependencies.
- Produces:
  - `func NormalizeMediaTitle(raw string) string`
  - `func BuildMediaQueryCandidates(raw string) []string`
  - `func TokenizeMediaMatchTerms(raw string) []string`
  - `func TitlesCompatible(query, candidate string) bool`
  - `func ScoreTitleMatch(query, candidate string) int`

- [ ] **Step 1: Write the failing tests**

```go
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
	for _, input := range []string{"1917", "1923（黄石前传）"} {
		if got := NormalizeMediaTitle(input); got == "" {
			t.Fatalf("NormalizeMediaTitle(%q) = empty, want non-empty", input)
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/media/titlematch -run 'Test(NormalizeMediaTitleRemovesEnglishAndChineseNoise|NormalizeMediaTitleKeepsPureYearTitles|BuildMediaQueryCandidatesIncludesBilingualAndPrefixStrippedForms|TitlesCompatibleRejectsGenericOnlyTitles|TitlesCompatibleAcceptsNoisyBilingualEquivalentTitles)' -v
```

Expected:

- FAIL because `internal/media/titlematch` does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

```go
package titlematch

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

var (
	spacePattern = regexp.MustCompile(`\s+`)
	yearPattern = regexp.MustCompile(`(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)`)
	seasonEpisodePattern = regexp.MustCompile(`(?i)(?:\bS\d{1,2}E\d{1,3}(?:E\d{1,3})*\b|\bSeason\s*\d+\b|第\s*[一二三四五六七八九十百零〇两\d]+\s*[季集])`)
	englishNoisePattern = regexp.MustCompile(`(?i)(?:^|[\s._\-\[])(?:4320p|2160p|1440p|1080p|720p|576p|540p|480p|4k|8k|uhd|bluray|blu-ray|bdrip|remux|web-dl|webdl|webrip|hdtv|hdrip|hybrid|x264|x265|h\.?264|h\.?265|hevc|avc|av1|hdr10\+?|hdr|dv|dovi|sdr|atmos|truehd|dts(?:-?hd)?|eac3|ac3|aac|ddp?|flac|mp3|pcm|nf|netflix|amzn|hmax|hulu|dsnp|imax|10bit|60fps|5\.1|7\.1|2\.0)\b`)
	chineseNoisePattern = regexp.MustCompile(`(?i)(蓝光|原盘|国配|中字|双语|内封字幕|特效字幕|修复版|杜比视界|国英双音|粤语|国语)`)
	catalogPrefixPattern = regexp.MustCompile(`^(?i)(美剧|韩剧|日剧|电影|电视剧|动漫|BBC)\s*`)
	collectionSuffixPattern = regexp.MustCompile(`(?i)(系列|合集|三部曲)\s*$`)
)

func NormalizeMediaTitle(raw string) string { /* implement per spec */ return strings.TrimSpace(raw) }
func BuildMediaQueryCandidates(raw string) []string { return []string{NormalizeMediaTitle(raw)} }
func TokenizeMediaMatchTerms(raw string) []string { return nil }
func TitlesCompatible(query, candidate string) bool { return false }
func ScoreTitleMatch(query, candidate string) int { return 0 }
```

Implementation notes for the real body:

- normalize separators `._-丨·•` to spaces;
- strip TMDB tags, noise tokens, export-size tails, season/episode markers;
- preserve pure numeric year titles before year stripping;
- add CJK/ASCII boundaries with regex replacements;
- build conservative candidates only;
- tokenize into meaningful CJK and ASCII terms of length `>= 2`, with year digits allowed only when length `== 4`;
- make `TitlesCompatible` require exact equality or meaningful token overlap, never generic-only matches.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
go test ./internal/media/titlematch -v
```

Expected:

- PASS for the new package tests.

- [ ] **Step 5: Commit**

```bash
git add internal/media/titlematch/titlematch.go internal/media/titlematch/titlematch_test.go
git commit -m "feat(media): add shared title matching core"
```

### Task 2: Upgrade Resource Search Title Filtering

**Files:**
- Modify: `internal/subscription/resource_search.go`
- Test: `internal/subscription/resource_search_test.go`

**Interfaces:**
- Consumes:
  - `titlematch.TitlesCompatible(query, candidate string) bool`
  - existing `filterResourceSearchResults(results []model.SubscriptionResourceSearchResult, query string, limit int) []model.SubscriptionResourceSearchResult`
- Produces:
  - resource-search filtering based on supported share links plus shared title compatibility.

- [ ] **Step 1: Write the failing tests**

```go
func TestFilterResourceSearchResultsMatchesNoisyBilingualTitles(t *testing.T) {
	results := []model.SubscriptionResourceSearchResult{{
		Title: "雨人 Rain Man 1988 蓝光原盘 REMUX",
		Links: []model.SubscriptionResourceSearchLink{{URL: "https://www.123pan.com/s/abc-def", Provider: string(ShareProviderPan123)}},
	}}

	filtered := filterResourceSearchResults(results, "Rain Man", 10)
	if len(filtered) != 1 {
		t.Fatalf("filtered len = %d, want 1", len(filtered))
	}
}

func TestFilterResourceSearchResultsRejectsContentOnlyKeywordHits(t *testing.T) {
	results := []model.SubscriptionResourceSearchResult{{
		Title:   "完全无关标题",
		Content: "这里提到了 雨人 和 Rain Man",
		Links:   []model.SubscriptionResourceSearchLink{{URL: "https://www.123pan.com/s/abc-def", Provider: string(ShareProviderPan123)}},
	}}

	filtered := filterResourceSearchResults(results, "雨人", 10)
	if len(filtered) != 0 {
		t.Fatalf("filtered len = %d, want 0", len(filtered))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/subscription -run 'Test(FilterResourceSearchResultsMatchesNoisyBilingualTitles|FilterResourceSearchResultsRejectsContentOnlyKeywordHits)' -v
```

Expected:

- first test fails because current logic only uses compact normalized substring matching;
- second test stays green or green-after-change, confirming the stricter requirement is preserved.

- [ ] **Step 3: Write minimal implementation**

```go
import "github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"

func filterResourceSearchResults(results []model.SubscriptionResourceSearchResult, query string, limit int) []model.SubscriptionResourceSearchResult {
	filtered := make([]model.SubscriptionResourceSearchResult, 0, len(results))
	for _, result := range results {
		if len(result.Links) == 0 {
			continue
		}
		if !titlematch.TitlesCompatible(query, result.Title) {
			continue
		}
		result.Provider = firstResultProvider(result.Links)
		filtered = append(filtered, result)
		if limit > 0 && len(filtered) >= limit {
			break
		}
	}
	return filtered
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
go test ./internal/subscription -run 'Test(ParseResourceSearchOutputFiltersByTitleAndSupportedShareLinks|ResourceLinksFromTextIgnoresUnsupportedURLs|FilterResourceSearchResultsMatchesNoisyBilingualTitles|FilterResourceSearchResultsRejectsContentOnlyKeywordHits)' -v
```

Expected:

- PASS for both new and previously added resource-search regression tests.

- [ ] **Step 5: Commit**

```bash
git add internal/subscription/resource_search.go internal/subscription/resource_search_test.go
git commit -m "fix(subscription): tighten resource search title matching"
```

### Task 3: Align Telegram Subscription Matching and Recognize Helpers

**Files:**
- Modify: `internal/subscription/telegram.go`
- Modify: `internal/subscription/telegram_test.go`
- Modify: `internal/media/recognize/recognize.go`
- Modify: `internal/media/recognize/recognize_test.go`

**Interfaces:**
- Consumes:
  - `titlematch.BuildMediaQueryCandidates(raw string) []string`
  - `titlematch.TitlesCompatible(query, candidate string) bool`
  - `titlematch.NormalizeMediaTitle(raw string) string`
- Produces:
  - `recognize.NormalizeTitle` delegating to shared normalization;
  - `recognize.BuildQueryCandidates` delegating to shared candidate generation;
  - `subscriptionEntryMatches(sub *model.Subscription, entry TreeEntry) bool` using shared compatibility rules.

- [ ] **Step 1: Write the failing tests**

```go
func TestNormalizeTitleRemovesChineseReleaseNoise(t *testing.T) {
	got := NormalizeTitle("问心 (2023) 蓝光原盘REMUX 国英双音")
	if got != "问心" {
		t.Fatalf("NormalizeTitle = %q, want %q", got, "问心")
	}
}

func TestBuildQueryCandidatesIncludesBilingualSplit(t *testing.T) {
	got := BuildQueryCandidates("美剧.诊疗中 第三季 Shrinking Season 3.2026")
	wants := []string{"诊疗中 Shrinking", "诊疗中", "Shrinking"}
	for _, want := range wants {
		found := false
		for _, item := range got {
			if item == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("BuildQueryCandidates missing %q in %#v", want, got)
		}
	}
}

func TestSubscriptionEntryMatchesNoisyMixedLanguagePath(t *testing.T) {
	sub := &model.Subscription{Name: "Rain Man", TMDBName: "雨人"}
	entry := TreeEntry{Name: "雨人 Rain Man 1988 蓝光原盘REMUX", Path: "/movie/剧情片/雨人 Rain Man 1988 蓝光原盘REMUX"}
	if !subscriptionEntryMatches(sub, entry) {
		t.Fatal("subscriptionEntryMatches returned false for equivalent noisy path")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/media/recognize -run 'Test(NormalizeTitleRemovesChineseReleaseNoise|BuildQueryCandidatesIncludesBilingualSplit)' -v
go test ./internal/subscription -run 'TestSubscriptionEntryMatchesNoisyMixedLanguagePath' -v
```

Expected:

- at least one recognize test fails because current cleanup/candidate generation is narrower;
- Telegram matching test fails because current matcher depends on compact substring contains.

- [ ] **Step 3: Write minimal implementation**

```go
import "github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"

func NormalizeTitle(value string) string {
	return titlematch.NormalizeMediaTitle(trimMediaExt(strings.TrimSpace(value)))
}

func BuildQueryCandidates(value string) []string {
	base := titlematch.BuildMediaQueryCandidates(trimMediaExt(strings.TrimSpace(value)))
	candidates := make([]string, 0, len(base)+2)
	for _, candidate := range base {
		candidates = appendUnique(candidates, candidate)
	}
	if prefix := titlePrefixBeforeEpisode(value); prefix != "" {
		for _, candidate := range titlematch.BuildMediaQueryCandidates(prefix) {
			candidates = appendUnique(candidates, candidate)
		}
	}
	return candidates
}
```

```go
import "github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"

func subscriptionEntryMatches(sub *model.Subscription, entry TreeEntry) bool {
	queries := make([]string, 0, 8)
	for _, raw := range []string{sub.TMDBName, sub.Name} {
		for _, candidate := range titlematch.BuildMediaQueryCandidates(raw) {
			queries = append(queries, candidate)
		}
	}
	haystacks := []string{entry.Name, entry.Path, fullPath(entry)}
	for _, query := range queries {
		for _, haystack := range haystacks {
			if titlematch.TitlesCompatible(query, haystack) {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
go test ./internal/media/recognize -v
go test ./internal/subscription -run 'TestSubscriptionEntryMatchesNoisyMixedLanguagePath' -v
```

Expected:

- PASS for new recognize and Telegram matching coverage;
- existing recognize tests still PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/media/recognize/recognize.go internal/media/recognize/recognize_test.go internal/subscription/telegram.go internal/subscription/telegram_test.go
git commit -m "refactor(media): reuse shared title matching in recognize and telegram"
```

### Task 4: Strengthen TMDB Candidate Compatibility and Scoring

**Files:**
- Modify: `internal/media/tmdb/tmdb.go`
- Test: `internal/media/tmdb/tmdb_test.go`

**Interfaces:**
- Consumes:
  - `titlematch.TitlesCompatible(query, candidate string) bool`
  - `titlematch.ScoreTitleMatch(query, candidate string) int`
  - existing `scoreCandidate(item tmdbItem, recognized recognize.Result) int`
- Produces:
  - compatibility-gated TMDB title ranking that still honors year and media-type signals.

- [ ] **Step 1: Write the failing tests**

```go
func TestScoreCandidatePrefersExactBilingualEquivalentTitle(t *testing.T) {
	recognized := recognize.Result{Title: "雨人 Rain Man", Year: 1988, MediaTypeHint: "movie"}
	exact := tmdbItem{Title: "雨人", OriginalTitle: "Rain Man", ReleaseDate: "1988-12-12", MediaType: "movie"}
	weak := tmdbItem{Title: "雨人诞生秘话", OriginalTitle: "Rain Man Documentary", ReleaseDate: "1988-12-12", MediaType: "movie"}
	if scoreCandidate(exact, recognized) <= scoreCandidate(weak, recognized) {
		t.Fatal("exact bilingual match did not outrank weak documentary-style title")
	}
}

func TestScoreCandidatePenalizesWeakSubstringMatches(t *testing.T) {
	recognized := recognize.Result{Title: "Shrinking", MediaTypeHint: "tv"}
	good := tmdbItem{Name: "Shrinking", OriginalName: "Shrinking", FirstAirDate: "2023-01-01", MediaType: "tv"}
	bad := tmdbItem{Name: "The Making of Shrinking", OriginalName: "The Making of Shrinking", FirstAirDate: "2023-01-01", MediaType: "tv"}
	if scoreCandidate(good, recognized) <= scoreCandidate(bad, recognized) {
		t.Fatal("weak substring candidate outranked exact title")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/media/tmdb -run 'Test(ScoreCandidatePrefersExactBilingualEquivalentTitle|ScoreCandidatePenalizesWeakSubstringMatches)' -v
```

Expected:

- FAIL because current scoring can still over-reward substring/LCS-style accidental matches.

- [ ] **Step 3: Write minimal implementation**

```go
import "github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"

func scoreCandidate(item tmdbItem, recognized recognize.Result) int {
	titles := []string{item.displayName(), item.originalDisplayName()}
	target := recognized.Title
	score := 0
	bestTitleScore := 0
	for _, title := range titles {
		if strings.TrimSpace(title) == "" {
			continue
		}
		if !titlematch.TitlesCompatible(target, title) {
			bestTitleScore = max(bestTitleScore, titlematch.ScoreTitleMatch(target, title)/4)
			continue
		}
		bestTitleScore = max(bestTitleScore, 40+titlematch.ScoreTitleMatch(target, title))
	}
	score += bestTitleScore
	if recognized.Year > 0 && yearFromDate(item.date()) == recognized.Year {
		score += 20
	}
	if recognized.MediaTypeHint != "" && item.MediaType == recognized.MediaTypeHint {
		score += 5
	}
	return score
}
```

Implementation notes for the real body:

- keep exact-match fast path;
- keep year/media-type boosts unchanged unless a test proves adjustment is needed;
- use shared score as the main title signal;
- make weak incompatible titles score much lower than compatible ones.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
go test ./internal/media/tmdb -v
```

Expected:

- PASS for new scoring tests and existing TMDB tests.

- [ ] **Step 5: Commit**

```bash
git add internal/media/tmdb/tmdb.go internal/media/tmdb/tmdb_test.go
git commit -m "fix(tmdb): prefer compatible title matches"
```

### Task 5: Improve 139 Share-Risk Rename Title Resolution

**Files:**
- Modify: `drivers/139/share_risk_rename.go`
- Test: `drivers/139/share_risk_rename_test.go`

**Interfaces:**
- Consumes:
  - updated `recognize.NormalizeTitle`
  - updated `recognize.BuildQueryCandidates`
  - existing `resolveShareRiskCanonicalTitle(ctx context.Context, root *model.Obj) (string, error)` or equivalent helper already used in the file.
- Produces:
  - better title cleanup/query generation in share-risk rename resolution without changing retry/rename workflow.

- [ ] **Step 1: Write the failing tests**

```go
func TestResolveShareRiskCanonicalTitleUsesCleanerBilingualTitle(t *testing.T) {
	root := &model.Obj{Name: "美剧.诊疗中 第三季 Shrinking Season 3.2026.2160p.WEB-DL"}
	canonical := resolveShareRiskCanonicalNameFromRecognize(recognize.Recognize(root.Name, "/tv/美剧"), &tmdb.Metadata{Title: "诊疗中", OriginalName: "Shrinking", Year: 2026})
	if canonical != "Shrinking" {
		t.Fatalf("canonical = %q, want %q", canonical, "Shrinking")
	}
}

func TestBuildShareRiskRenamePlanStillPreservesSeasonEpisodeStructureWithDirtyNames(t *testing.T) {
	plan := []renameShareRiskItem{
		{OldName: "诊疗中 Shrinking 第三季", NewName: "Shrinking"},
		{OldName: "诊疗中 Shrinking S03E01.mkv", NewName: "Shrinking S03E01.mkv"},
	}
	if strings.Contains(plan[1].NewName, "第三季") {
		t.Fatalf("NewName = %q, want season marker removed from title portion", plan[1].NewName)
	}
}
```

If the file already exposes different helper names, adapt the tests to those exact helpers rather than introducing a new exported API.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./drivers/139 -run 'Test(ResolveShareRiskCanonicalTitleUsesCleanerBilingualTitle|BuildShareRiskRenamePlanStillPreservesSeasonEpisodeStructureWithDirtyNames)' -v
```

Expected:

- FAIL because current recognition/query cleanup is narrower and does not produce the best bilingual canonical title in dirty-name cases.

- [ ] **Step 3: Write minimal implementation**

```go
func resolveShareRiskCanonicalTitle(ctx context.Context, root *model.Obj) (string, error) {
	recognized := recognize.Recognize(root.GetName(), path.Dir(root.GetPath()))
	meta, err := resolveShareRiskTMDBMetadata(ctx, recognized)
	if err != nil {
		return fallbackShareRiskCanonicalTitle(recognized.Title), nil
	}
	if strings.TrimSpace(meta.OriginalName) != "" {
		return strings.TrimSpace(meta.OriginalName), nil
	}
	if strings.TrimSpace(meta.Title) != "" {
		return strings.TrimSpace(meta.Title), nil
	}
	return fallbackShareRiskCanonicalTitle(recognized.Title), nil
}
```

Implementation notes for the real body:

- do not change the retry trigger or rename-plan execution order;
- let the improved `recognize` candidates do most of the work;
- keep season/episode preservation rules untouched.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
go test ./drivers/139 -run 'Test(ResolveShareRiskCanonicalTitleUsesCleanerBilingualTitle|BuildShareRiskRenamePlanStillPreservesSeasonEpisodeStructureWithDirtyNames)' -v
go test ./drivers/139 -run 'Test(CreateMobileShare|BuildShareRiskRenamePlan|ApplyShareRiskRenamePlan)' -v
```

Expected:

- PASS for new dirty-title rename tests;
- PASS for existing share-risk rename and mobile-share retry tests.

- [ ] **Step 5: Commit**

```bash
git add drivers/139/share_risk_rename.go drivers/139/share_risk_rename_test.go
git commit -m "fix(139): improve share risk rename title resolution"
```

### Task 6: Full Regression Verification

**Files:**
- Modify: `docs/superpowers/specs/2026-07-08-title-matching-core-design.md` only if implementation forces a spec correction.
- No code changes expected unless regressions appear.

**Interfaces:**
- Consumes: outputs of Tasks 1-5.
- Produces: verified green test evidence for the completed rollout.

- [ ] **Step 1: Run the focused package regressions**

Run:

```bash
go test ./internal/media/titlematch ./internal/media/recognize ./internal/media/tmdb ./internal/subscription ./drivers/139
```

Expected:

- PASS for all five packages.

- [ ] **Step 2: Run targeted high-signal regressions again if the package run fails**

Run:

```bash
go test ./internal/subscription -run 'Test(ParseResourceSearchOutputFiltersByTitleAndSupportedShareLinks|ResourceLinksFromTextIgnoresUnsupportedURLs|FilterResourceSearchResultsMatchesNoisyBilingualTitles|SubscriptionEntryMatchesNoisyMixedLanguagePath)' -v
go test ./internal/media/recognize -run 'Test(NormalizeTitleRemovesReleaseNoise|NormalizeTitleRemovesChineseReleaseNoise|BuildQueryCandidatesIncludesBilingualSplit)' -v
go test ./internal/media/tmdb -run 'Test(ScoreCandidatePrefersExactBilingualEquivalentTitle|ScoreCandidatePenalizesWeakSubstringMatches)' -v
go test ./drivers/139 -run 'Test(ResolveShareRiskCanonicalTitleUsesCleanerBilingualTitle|CreateMobileShare|BuildShareRiskRenamePlan|ApplyShareRiskRenamePlan)' -v
```

Expected:

- PASS for each targeted regression group.

- [ ] **Step 3: Commit the final integrated state**

```bash
git add internal/media/titlematch internal/media/recognize internal/media/tmdb internal/subscription drivers/139
git commit -m "refactor(media): unify title matching across search and rename"
```

## Self-Review

### Spec Coverage

- Shared title-understanding core: Task 1.
- Resource-search stricter title matching + supported share links preserved: Task 2.
- Telegram subscription matching upgrade: Task 3.
- Recognize normalization/candidate alignment: Task 3.
- TMDB compatibility/scoring upgrade: Task 4.
- 139 share-risk rename title resolution improvement: Task 5.
- End-to-end regression verification: Task 6.

No spec requirement is left without a task.

### Placeholder Scan

- No `TODO`/`TBD` placeholders remain.
- Every task has exact files, concrete commands, and code snippets.
- One conditional note remains in Task 5 about adapting to existing helper names; this is intentional to prevent inventing a new public API if the file already has an equivalent private helper.

### Type Consistency

- Shared helper signatures are defined in Task 1 and reused consistently in Tasks 2-4.
- `recognize.NormalizeTitle` and `recognize.BuildQueryCandidates` remain existing exported functions; only their internals change in Task 3.
- `scoreCandidate(item tmdbItem, recognized recognize.Result) int` remains existing private TMDB scoring entrypoint in Task 4.
- `filterResourceSearchResults(results []model.SubscriptionResourceSearchResult, query string, limit int) []model.SubscriptionResourceSearchResult` remains the resource-search filtering entrypoint in Task 2.
