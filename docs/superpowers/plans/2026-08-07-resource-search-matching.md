# Cross-Source Resource Search and Matching Implementation Plan

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Borrow the useful design principles from mediary-scout to make OpenList resource discovery more reliable for noisy PanSou names while preserving HDHive TMDB-ID matching, existing provider priority, and current storage workflows.

**Architecture:** Keep internal/media/titlematch as the shared title-identity layer and add a small deterministic release-name parser plus a subscription-level target-aware matcher. PanSou and Telegram candidates will be normalized, filtered, ranked, and then passed through the existing share inspection, source-priority selection, transfer, and actual-file verification paths. HDHive remains an ID-based provider and will not be forced through fuzzy title matching.

**Tech Stack:** Go, standard-library regular expressions and sorting, existing OpenList subscription models and share providers, existing internal/media/titlematch, internal/media/recognize, httptest, and the current Go test suite.

## Implementation Record (2026-08-07)

- Implemented on `task/storage-semantic-unification` in commits `7f79899d`, `4252e056`, `419f43b2`, and `c07ea800`.
- Added a shared release-name parser, target-aware PanSou matching/ranking, duplicate-link suppression, automatic standalone/cluster integration, and Telegram season/episode fallback reuse.
- Preserved HDHive's `media_type + tmdb_id` lookup semantics; its regression fixture intentionally returns an unrelated display title while requiring the TMDB-ID route.
- Verified successfully on the merged branch:
  - `go test ./internal/media/... ./internal/subscription ./internal/hdhive ./internal/cluster ./drivers/115_sy ./drivers/115 ./drivers/115_open`
  - `go vet ./internal/media/release ./internal/subscription ./internal/hdhive ./internal/cluster`
  - `go test -race ./internal/media/release ./internal/subscription ./internal/hdhive ./internal/cluster`
  - `git diff --check`
- `go test ./...` was run and returned non-zero because of unrelated existing/environment-dependent failures: non-constant format vet diagnostics in untouched packages, missing system `fuse.h`, an environment-specific `internal/net` proxy test, and Aria2 RPC tests requiring a service on `localhost:6800`. The changed packages remain green in the targeted command above.

## Global Constraints

- Do not add a new third-party dependency.
- Do not change the existing HTTP request or response schema in this iteration.
- Do not modify the 115sy driver or any other storage driver as part of this feature.
- Reuse the existing internal/media/titlematch package; do not create a second title-normalization implementation.
- HDHive matching remains media_type + tmdb_id; do not reject an HDHive result because its display title is noisy.
- PanSou and Telegram results must still contain a real supported share link before they are accepted.
- Quality, codec, and subtitle tokens are post-recall evidence, not mandatory initial search-keyword tokens.
- Explicit conflicting title, year, season, or episode evidence must be rejected before quality or file-size preference is considered.
- Existing provider priority remains stronger than same-provider size and tie-break rules.
- Matching must be deterministic and explainable; an LLM integration is explicitly out of scope.
- Do not log share passwords, signed URLs, proxy secrets, or complete raw search payloads.
- Add regression tests before changing behavior and preserve existing idempotency, slot closure, and cluster dispatch semantics.

## Current Baseline

The branch already contains the shared internal/media/titlematch core, including normalization, bilingual candidates, compatibility checks, and title scoring. internal/subscription/resource_search.go filters search results by supported links and title compatibility. internal/subscription/telegram.go matches temporary files and selects one candidate per episode using provider priority, then size. HDHive already searches by TMDB ID and has its own unlock/cache service.

The gap is that PanSou results are still treated mostly as title, content, and links. There is no shared structured representation for release-name fields such as year, season, episode range, completeness, quality, or subtitle evidence, and the automatic PanSou path does not rank candidates against the subscription target before inspecting links.

## File Structure

- Create: internal/media/release/release.go
  - Parse noisy release names into title candidates, year, season, episode range, completeness, quality, source, audio, and subtitle evidence.
- Create: internal/media/release/release_test.go
  - Table-driven parser tests for Chinese, English, bilingual, movie, TV, multi-episode, and noisy PanSou names.
- Create: internal/subscription/resource_match.go
  - Build a subscription/search target, normalize resource evidence, make hard match decisions, and provide stable ranking.
- Create: internal/subscription/resource_match_test.go
  - Tests for title identity, year/season/episode conflicts, coverage, completeness, and stable tie-breaking.
- Modify: internal/subscription/resource_search.go
  - Apply target-aware ranking to PanSou results while preserving generic manual search and HDHive ID-based behavior.
- Modify: internal/subscription/resource_search_test.go
  - Add ranked PanSou fixtures, duplicate-link cases, and HDHive bypass regressions.
- Modify: internal/subscription/service.go
  - Use the target-aware PanSou search path for automatic standalone subscriptions and preserve existing item identity.
- Modify: internal/subscription/cluster_run.go
  - Use the same target-aware PanSou search path before creating cluster inspection observations.
- Modify: internal/subscription/telegram.go
  - Reuse parsed release season/episode evidence before the existing recognition fallback; keep provider-priority selection unchanged.
- Modify: internal/subscription/telegram_test.go
  - Add noisy release-name season/episode matching tests.
- Modify: internal/subscription/share_inspect_transfer_test.go
  - Verify that improved candidate filtering does not change accepted-source and slot-closure behavior.
- Modify: internal/subscription/cluster_dispatch_test.go
  - Verify cluster PanSou observations receive only accepted, target-matched links and retain existing observation idempotency.

## Task 1: Add the Structured Release-Name Parser

**Files:**
- Create: internal/media/release/release.go
- Test: internal/media/release/release_test.go

**Interfaces:**
- Consumes: raw PanSou titles, Telegram file names, and optional parent-path text.
- Produces:

~~~go
package release

type ParsedName struct {
    Raw             string
    Title           string
    TitleCandidates []string
    Year            int
    Season          int
    EpisodeStart    int
    EpisodeEnd      int
    Complete        bool
    Quality         string
    Source          string
    Audio           []string
    Subtitles       []string
}

func Parse(raw string) ParsedName
~~~

- [ ] Step 1: Write failing parser tests.

Add table-driven cases with exact expected fields:

~~~go
func TestParseExtractsBilingualMovieFields(t *testing.T) {
    got := Parse("雨人 Rain Man (1988) 1080p BluRay REMUX 国英双语 简中")
    if got.Title != "雨人 Rain Man" || got.Year != 1988 || got.Quality != "1080p" {
        t.Fatalf("parsed movie = %#v", got)
    }
    if !contains(got.Audio, "国英双语") || !contains(got.Subtitles, "简中") {
        t.Fatalf("parsed language evidence = %#v", got)
    }
}

func TestParseExtractsSeasonAndEpisodeRange(t *testing.T) {
    got := Parse("权力的游戏.Game.of.Thrones.S02E01-E10.1080p.WEB-DL.中英字幕")
    if got.Season != 2 || got.EpisodeStart != 1 || got.EpisodeEnd != 10 {
        t.Fatalf("parsed range = %#v", got)
    }
    if got.Title != "权力的游戏 Game of Thrones" {
        t.Fatalf("parsed title = %q", got.Title)
    }
}

func TestParseSupportsChineseSeasonAndCompleteMarkers(t *testing.T) {
    got := Parse("国产剧 斗破苍穹 第五季 104集全 4K HDR")
    if got.Season != 5 || got.EpisodeEnd != 104 || !got.Complete {
        t.Fatalf("parsed Chinese TV = %#v", got)
    }
}

func TestParseDoesNotTreatYearOnlyMovieAsReleaseNoise(t *testing.T) {
    got := Parse("1917")
    if got.Title != "1917" || got.Year != 0 {
        t.Fatalf("parsed year-only title = %#v", got)
    }
}

func contains(values []string, want string) bool {
    for _, value := range values {
        if value == want {
            return true
        }
    }
    return false
}
~~~

Run:

~~~bash
go test ./internal/media/release -v
~~~

Expected: the package does not exist yet and the command fails to compile.

- [ ] Step 2: Implement the parser with ordered extraction.

Implement Parse in this order so meaningful evidence is not deleted before it is read:

~~~go
func Parse(raw string) ParsedName {
    info := ParsedName{Raw: strings.TrimSpace(raw)}
    if info.Raw == "" {
        return info
    }
    value := trimMediaExtension(normalizeSeparators(info.Raw))
    info.Year = extractNonTitleYear(value)
    info.Season, info.EpisodeStart, info.EpisodeEnd = extractEpisodeEvidence(value)
    info.Complete = hasCompleteMarker(value)
    info.Quality = extractQuality(value)
    info.Source = extractSource(value)
    info.Audio = extractAudio(value)
    info.Subtitles = extractSubtitles(value)
    info.Title = titlematch.NormalizeMediaTitle(value)
    info.TitleCandidates = titlematch.BuildMediaQueryCandidates(value)
    if info.Title == "" && len(info.TitleCandidates) > 0 {
        info.Title = info.TitleCandidates[0]
    }
    return info
}
~~~

The implementation must remove common media extensions before title parsing and recognize S01E02, S01E01-E10, 1x02, Season 2, 第2季, 第10集, 10集全, 全集, and 更新至10集. It must extract quality and subtitle tokens but remove them only from the title representation. It must never classify 1917 or 1923 as a release year when the entire normalized value is a four-digit title.

Use titlematch.NormalizeMediaTitle and titlematch.BuildMediaQueryCandidates for title cleanup. Do not copy their regular expressions into the new package.

- [ ] Step 3: Run parser tests and format the new package.

~~~bash
gofmt -w internal/media/release/release.go internal/media/release/release_test.go
go test ./internal/media/release -v
~~~

Expected: all parser cases pass.

- [ ] Step 4: Commit the parser.

~~~bash
git add internal/media/release/release.go internal/media/release/release_test.go
git commit -m "feat(media): parse noisy release names"
~~~

## Task 2: Build Target-Aware Resource Matching and Ranking

**Files:**
- Create: internal/subscription/resource_match.go
- Test: internal/subscription/resource_match_test.go

**Interfaces:**
- Consumes: model.Subscription, model.SubscriptionResourceSearchResult, TreeEntry, release.ParsedName, and titlematch.
- Produces these package-private types and functions for the subscription package:

~~~go
type resourceMatchTarget struct {
    Titles       []string
    MediaType    string
    Year         int
    Seasons      map[int]struct{}
    Season       int
    EpisodeStart int
    EpisodeEnd   int
}

type resourceMatchCandidate struct {
    ID       string
    Title    string
    Path     string
    Provider string
    Size     int64
    Result   *model.SubscriptionResourceSearchResult
    Parsed   release.ParsedName
}

type resourceMatchDecision struct {
    Accepted        bool
    Score           int
    TitleScore      int
    EpisodeCoverage int
    Reasons         []string
    Parsed          release.ParsedName
}

func buildResourceMatchTarget(sub *model.Subscription, query string) resourceMatchTarget
func decideResourceMatch(target resourceMatchTarget, candidate resourceMatchCandidate) resourceMatchDecision
func rankResourceCandidates(target resourceMatchTarget, candidates []resourceMatchCandidate) []resourceMatchCandidate
~~~

- [ ] Step 1: Write failing matching tests.

Cover the following behavior:

~~~go
func TestDecideResourceMatchAcceptsNoisyBilingualTitle(t *testing.T) {
    target := buildResourceMatchTarget(&model.Subscription{
        TMDBName: "雨人", TMDBYear: 1988, MediaType: "movie",
    }, "")
    decision := decideResourceMatch(target, resourceMatchCandidate{
        Title: "雨人 Rain Man 1988 1080p WEB-DL 国英双语",
    })
    if !decision.Accepted || decision.Score <= 0 {
        t.Fatalf("decision = %#v", decision)
    }
}

func TestDecideResourceMatchRejectsExplicitWrongSeason(t *testing.T) {
    target := buildResourceMatchTarget(&model.Subscription{
        TMDBName: "示例剧", MediaType: "tv", Seasons: []int{2},
    }, "")
    decision := decideResourceMatch(target, resourceMatchCandidate{
        Title: "示例剧 S01E01 1080p",
    })
    if decision.Accepted {
        t.Fatalf("decision = %#v, want rejection", decision)
    }
}

func TestRankResourceCandidatesPrefersCompleteCoverageBeforeQuality(t *testing.T) {
    target := buildResourceMatchTarget(&model.Subscription{
        TMDBName: "示例剧", MediaType: "tv", Seasons: []int{1},
    }, "")
    candidates := []resourceMatchCandidate{
        {ID: "partial-4k", Title: "示例剧 S01E01-E04 4K"},
        {ID: "complete-1080p", Title: "示例剧 S01 1080p 12集全"},
    }
    ranked := rankResourceCandidates(target, candidates)
    if ranked[0].ID != "complete-1080p" {
        t.Fatalf("ranked = %#v, want complete pack first", ranked)
    }
}
~~~

Also test that generic-only queries, content-only keyword mentions, documentary suffixes, conflicting years, and non-matching titles are rejected.

- [ ] Step 2: Run matching tests to verify the new API is absent.

~~~bash
go test ./internal/subscription -run 'Test(DecideResourceMatch|RankResourceCandidates)' -v
~~~

Expected: compile failure because the new matching types and functions do not exist.

- [ ] Step 3: Implement hard gates and explainable scoring.

Build target title candidates from sub.TMDBName, sub.Name, and the explicit query using titlematch.BuildMediaQueryCandidates. Keep empty and generic-only candidates out of the target.

Apply the following decision order:

~~~text
1. Parse candidate title and release evidence.
2. Require a supported title candidate to be title-compatible.
3. Reject explicit documentary/special suffixes when the target is the base work.
4. Reject explicit conflicting year when both target and candidate years exist.
5. Reject explicit season outside selected subscription seasons.
6. Reject an explicit episode range that cannot cover the requested range.
7. Score title identity, year, season, coverage, completeness, and language evidence.
8. Keep unknown evidence accepted but lower-confidence; do not infer a conflict from absence.
~~~

Use a stable score with title identity dominant. The score must not let quality or size rescue a title or season conflict. Preserve the original candidate order when scores tie, and make rankResourceCandidates use sort.SliceStable.

The matcher must not alter SourceProvider priority. Provider priority continues to be applied by the existing selectTelegramTempTransferCandidates path after share inspection.

- [ ] Step 4: Run matching tests and static checks.

~~~bash
gofmt -w internal/subscription/resource_match.go internal/subscription/resource_match_test.go
go test ./internal/subscription -run 'Test(DecideResourceMatch|RankResourceCandidates)' -v
go test ./internal/media/titlematch ./internal/media/release ./internal/subscription
~~~

Expected: all new and existing matching tests pass.

- [ ] Step 5: Commit the matcher.

~~~bash
git add internal/subscription/resource_match.go internal/subscription/resource_match_test.go
git commit -m "feat(subscription): add target-aware resource matching"
~~~

## Task 3: Integrate Ranking into Manual and Automatic PanSou Search

**Files:**
- Modify: internal/subscription/resource_search.go
- Modify: internal/subscription/resource_search_test.go
- Modify: internal/subscription/service.go
- Modify: internal/subscription/cluster_run.go

**Interfaces:**
- Consumes: buildResourceMatchTarget, decideResourceMatch, rankResourceCandidates, existing PanSou parser, and existing share-link extraction.
- Produces:
  - generic manual PanSou search that filters and stably ranks accepted results by query compatibility;
  - subscription PanSou search that uses TMDB name/year/season/episode target data before link inspection;
  - identical target-aware ordering for standalone and cluster execution.

- [ ] Step 1: Add regression fixtures before changing the search path.

Extend internal/subscription/resource_search_test.go with a PanSou response containing:

~~~json
{
  "data": [
    {"title":"示例剧 S01E01-E04 4K","links":[{"url":"https://www.123pan.com/s/partial"}]},
    {"title":"示例剧 S01 1080p 12集全 中字","links":[{"url":"https://www.123pan.com/s/complete"}]},
    {"title":"完全无关的合集","content":"提到示例剧 https://www.123pan.com/s/noise"},
    {"title":"示例剧 S02 1080p","links":[{"url":"https://www.123pan.com/s/wrong-season"}]}
  ]
}
~~~

Assert that a season-1 subscription keeps the complete season-1 candidate first, rejects content-only noise, and rejects the explicit season-2 candidate. Add a duplicate-link fixture with two different noisy titles and assert that the link is processed once while the richer accepted title is retained.

- [ ] Step 2: Run the new fixtures against the current implementation.

~~~bash
go test ./internal/subscription -run 'Test(PanSou|ResourceSearch)' -v
~~~

Expected: the new ordering/season assertions fail or are not yet expressible, establishing the behavior gap.

- [ ] Step 3: Refactor PanSou search around an optional target.

Keep the current generic signature for existing callers and tests, then add one target-aware wrapper:

~~~go
func searchPanSouResources(ctx context.Context, query string, limit int, cfg model.SubscriptionPanSouSourceConfig) ([]model.SubscriptionResourceSearchResult, error) {
    return searchPanSouResourcesWithTarget(ctx, query, limit, cfg, nil)
}

func searchPanSouResourcesForSubscription(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionPanSouSourceConfig) ([]model.SubscriptionResourceSearchResult, error) {
    if sub == nil {
        return nil, errors.New("subscription is required")
    }
    query := telegramSearchQuery(sub)
    if query == "" {
        return nil, errors.New("pansou search query is required")
    }
    target := buildResourceMatchTarget(sub, query)
    return searchPanSouResourcesWithTarget(ctx, query, cfg.Limit, cfg, &target)
}
~~~

searchPanSouResourcesWithTarget must continue to:

- support the configured command and HTTP endpoint modes;
- retain GET-to-POST fallback behavior;
- discard results without supported share links;
- report provider errors to the caller instead of converting them to an empty success;
- preserve the raw title and content in the returned result.

After parsing and link validation, create internal candidates, apply decideResourceMatch, discard rejected candidates, and return the stable ranked result list. Do not add match fields to the HTTP response in this iteration.

- [ ] Step 4: Route automatic standalone and cluster PanSou runs through the target-aware function.

In internal/subscription/service.go, replace the runPanSou call to the generic search function with searchPanSouResourcesForSubscription(ctx, sub, cfg).

In internal/subscription/cluster_run.go, make the same replacement before building the observation items. Keep the observation key, link deduplication, source priority, and dispatch behavior unchanged.

The PanSou link item identity must continue to include the subscription ID, normalized result identity, and link URL so re-running the same search remains idempotent.

- [ ] Step 5: Run focused search and execution tests.

~~~bash
gofmt -w internal/subscription/resource_search.go internal/subscription/service.go internal/subscription/cluster_run.go internal/subscription/resource_search_test.go
go test ./internal/subscription -run 'Test(PanSou|ResourceSearch|RunPanSou|ClusterPanSou)' -v
~~~

Expected: the target-aware ranking tests pass, existing PanSou HTTP/command tests pass, and no cluster observation test reports duplicate or missing links.

- [ ] Step 6: Commit the PanSou integration.

~~~bash
git add internal/subscription/resource_search.go internal/subscription/resource_search_test.go internal/subscription/service.go internal/subscription/cluster_run.go
git commit -m "feat(subscription): rank PanSou resources by media target"
~~~

## Task 4: Reuse Release Evidence in File and Share Matching

**Files:**
- Modify: internal/subscription/telegram.go
- Modify: internal/subscription/telegram_test.go
- Modify: internal/subscription/share_inspect_transfer_test.go

**Interfaces:**
- Consumes: release.Parse, existing recognize.Recognize, subscriptionEntryMatches, and existing source-priority selection.
- Produces: better season/episode extraction for noisy share file names without changing the existing provider-priority or slot-closure policy.

- [ ] Step 1: Add failing file-name cases.

Add tests for entries such as:

~~~text
权力的游戏.Game.of.Thrones.S02E05.1080p.WEB-DL.mkv
示例剧 第2季 第05集 中字.mp4
示例剧 S02 05 1080p WEB-DL.mkv
~~~

Assert that entrySeasonEpisode and itemFromEntry derive season 2 and episode 5 where the current recognizer does not, while existing leading-episode and parent-directory fallbacks remain unchanged.

- [ ] Step 2: Run the focused tests before the fallback change.

~~~bash
go test ./internal/subscription -run 'Test(EntrySeasonEpisode|Subscription.*Match|ShareInspect)' -v
~~~

Expected: the new irregular-name cases fail or remain unrecognized.

- [ ] Step 3: Add release parsing as the first evidence source and recognition as fallback.

Update entrySeasonEpisode so it follows this exact order:

~~~go
parsed := release.Parse(entry.Name)
season, episode := parsed.Season, parsed.EpisodeStart
if season <= 0 || episode <= 0 {
    recognized := recognize.Recognize(entry.Name, parentPath(entry))
    if season <= 0 {
        season = recognized.Season
    }
    if episode <= 0 {
        episode = recognized.Episode
    }
}
// Preserve the existing parent-path, ExtractSeasonEpisode, and leading-number fallbacks.
~~~

Do not change subscriptionTitleMatches, selectTelegramTempTransferCandidates, or betterTelegramTempCandidate in this task except where a test proves that the parser must supply missing season/episode evidence. Existing selection order remains:

~~~text
provider priority → same-provider file size → stable key
~~~

- [ ] Step 4: Run the subscription regression suite.

~~~bash
gofmt -w internal/subscription/telegram.go internal/subscription/telegram_test.go
go test ./internal/subscription -run 'Test(EntrySeasonEpisode|Subscription|ShareInspect|SlotClose|TempDedup)' -v
~~~

Expected: irregular release-name tests pass and existing source-prefilter, slot-close, and temp-dedup tests remain green.

- [ ] Step 5: Commit the file matching integration.

~~~bash
git add internal/subscription/telegram.go internal/subscription/telegram_test.go internal/subscription/share_inspect_transfer_test.go
git commit -m "fix(subscription): improve release season matching"
~~~

## Task 5: Preserve HDHive Semantics and Complete End-to-End Verification

**Files:**
- Modify: internal/subscription/resource_search_test.go
- Modify: internal/hdhive/service_test.go only if a missing regression case is needed
- Modify: internal/subscription/cluster_dispatch_test.go only if a missing target-aware observation assertion is needed
- Modify: this plan only to record verified commands and known gaps after implementation

**Interfaces:**
- Consumes: the completed release parser, target matcher, PanSou integration, existing HDHive client/service, and subscription/cluster test fixtures.
- Produces: evidence that the feature improves PanSou matching without changing HDHive ID matching, provider priority, link validation, or post-transfer behavior.

- [ ] Step 1: Add HDHive identity regression coverage.

Use a fake HDHive client/server to assert that:

- media_type=tv and a valid tmdb_id return resources even when the display title is not equal to the input query;
- an invalid or missing TMDB ID is rejected;
- channel_115 still permits both 115 and ed2k resources;
- HDHive results are not discarded by PanSou title compatibility rules.

- [ ] Step 2: Add end-to-end PanSou subscription coverage.

Use httptest.Server for PanSou and the existing injected inspection functions to verify:

~~~text
messy PanSou response
  → wrong-title/content-only results rejected
  → correct season pack ranked first
  → accepted links inspected once
  → existing provider priority and episode-slot selection applied
  → cluster and standalone paths produce the same accepted link set
~~~

Do not perform real external network calls in unit tests.

- [ ] Step 3: Run the complete verification sequence.

Run sequentially:

~~~bash
go test ./internal/media/titlematch ./internal/media/release ./internal/media/recognize ./internal/media/tmdb
go test ./internal/subscription ./internal/hdhive ./internal/cluster
go test ./...
go vet ./...
~~~

Expected: all commands exit with status 0. If the repository's normal CI uses an additional formatter or build command, run that command as well and record the result in the implementation commit.

- [ ] Step 4: Review the diff and verify the scope.

Confirm:

- no storage-driver files changed;
- no HTTP request/response fields were added or removed;
- HDHive search still uses TMDB ID rather than title matching;
- PanSou rejected results do not create subscription items or cluster inspect jobs;
- source priority still beats same-source size;
- no secrets or complete share URLs are logged;
- the old title-matching tests and subscription retry/idempotency tests remain unchanged and pass.

- [ ] Step 5: Commit final verification changes.

~~~bash
git add internal/media/release internal/subscription internal/hdhive docs/superpowers/plans/2026-08-07-resource-search-matching.md
git commit -m "test(subscription): verify cross-source resource matching"
~~~

## Acceptance Criteria

- A PanSou result with bilingual titles and release-name noise matches the correct subscription target.
- A result that mentions the target only in content but has an unrelated title is rejected.
- Explicit wrong-year, wrong-season, documentary-suffix, and generic-only results are rejected.
- Complete season packs rank ahead of partial packs when the title identity is equivalent, regardless of partial-pack quality.
- HDHive continues to match by media_type + tmdb_id and is not filtered by PanSou title rules.
- Manual PanSou search and automatic standalone/cluster PanSou subscriptions use the same matching semantics.
- Existing source priority remains stronger than same-provider size and stable tie-break rules.
- Accepted PanSou links are inspected and transferred through existing workflows only; rejected links never create transfer work.
- Existing 115sy, Telegram, share inspection, slot closure, cluster observation, and idempotency behavior remains intact.

## Explicit Non-Goals

- No LLM Agent or vector search integration.
- No automatic quality/subtitle preference configuration UI.
- No full provider-independent search cache in this iteration.
- No changes to 115sy download, move, copy, delete, or mount logic.
- No replacement of the existing internal/media/titlematch core.
