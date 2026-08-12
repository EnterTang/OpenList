# Shared Title Matching Core Design

## Goal

Introduce one shared, testable title-understanding core for OpenList and use it to improve four existing behaviors without changing external APIs:

- make `订阅源搜索` title matching stricter and less noisy;
- improve subscription temp-transfer matching for mixed Chinese / English / noisy filenames;
- improve TMDB candidate selection so exact or strongly compatible titles win more reliably;
- improve `139Yun` share-risk rename title resolution so dirty names still produce stable TMDB or fallback rename targets.

The new work should unify how OpenList cleans titles, builds query candidates, tokenizes mixed-language names, and decides whether two titles are compatible enough to be treated as the same media work.

## Non-Goals

- Do not import the full archive / category / folder-template system from `yidong139_pan_manager`.
- Do not change HTTP request or response schemas.
- Do not introduce a full anime-specific rename or archive pipeline in this iteration.
- Do not redesign the existing TMDB client API surface.
- Do not weaken the existing requirement that resource-search results must contain a real supported share link.

## Existing Context

Current related logic is split across several places:

- `internal/subscription/resource_search.go`
  - result parsing;
  - share-link extraction;
  - current title filter, which is still a simple normalized substring check.
- `internal/subscription/telegram.go`
  - `subscriptionEntryMatches` currently uses simple normalized `contains` matching.
- `internal/media/recognize/recognize.go`
  - `NormalizeTitle` and `BuildQueryCandidates` already do basic filename cleanup.
- `internal/media/tmdb/tmdb.go`
  - candidate scoring already uses normalized title comparison, token overlap, and substring / LCS-like logic.
- `drivers/139/share_risk_rename.go`
  - rename planning currently depends on `recognize` + `tmdb`, so any shared title improvements should help it indirectly or through small adapter changes.

The user also provided a reference implementation from `yidong139_pan_manager`. The useful parts to borrow are the title-cleaning and candidate-building heuristics, not its archive-specific business rules.

## Product Decisions

The following scope decisions are approved for this work:

- Build a shared title-understanding core and reuse it across search, recognition, TMDB matching, and 139 share-risk rename.
- Prefer small, composable helpers over one large feature-specific pipeline.
- Keep the first iteration deterministic and rule-based.
- Add tests before each behavioral change.
- Keep existing public behavior stable except where the user explicitly wants stricter filtering or better matching.

## Recommended Approach

Create one small shared helper module under `internal/media` dedicated to title understanding. The exact filename may vary, but responsibilities should be split roughly as follows:

- normalize noisy media titles;
- generate search / comparison candidates from one raw title;
- tokenize mixed Chinese / English titles into comparison terms;
- compare two titles for compatibility;
- optionally score candidate titles for ranking use cases.

Then integrate that module into existing call sites rather than replacing entire subsystems.

This keeps the change aligned with OpenList’s current architecture while borrowing the strongest ideas from `yidong139_pan_manager`.

## Borrowed Heuristics

The new shared logic should borrow the following ideas from `yidong139_pan_manager` and adapt them to Go:

### Title Normalization

Expand current cleaning beyond the existing `recognize.NormalizeTitle` rules:

- strip embedded TMDB markers;
- strip common English release noise such as resolution, codecs, HDR, WEB-DL, REMUX, audio tags, streaming tags, and release-group tails;
- strip common Chinese noise such as 蓝光, 原盘, 国配, 双语, 中字, 内封字幕, 修复版;
- strip size / export metadata tails;
- strip season / episode markers while preserving pure-title use cases;
- keep pure numeric year titles like `1917` or `1923` intact;
- insert boundaries between ASCII and CJK characters so `E.T.外星人`-style inputs become tokenizable;
- include a very small set of proven OCR / typo repairs only where tests justify them.

### Query Candidate Generation

Generate multiple deterministic candidates from one raw title:

- normalized base title;
- title with common catalog prefixes removed, such as `美剧`, `韩剧`, `电影`, `BBC`-style broadcast prefixes where appropriate;
- bilingual splits:
  - CJK-only candidate;
  - ASCII-only candidate;
- collection-suffix stripped candidate for cases like `系列`, `合集`, `三部曲`;
- selected CJK suffix segments for long Chinese titles where the full title is too noisy.

The candidate builder must stay conservative: candidates should help matching, not explode into dozens of vague aliases.

### Compatibility and Matching

Replace the weakest current substring-only checks with compatibility-aware matching:

- exact normalized equality wins immediately;
- token overlap is stronger than raw substring matching;
- CJK titles should be compared using both full normalized strings and conservative segment overlap;
- English matches should require meaningful token overlap, not only character containment;
- generic candidates such as `第三季`, `合集`, or year-only noise should not count as title matches.

The compatibility layer should return a boolean for filtering scenarios and optionally expose a simple score for ranking scenarios.

## Shared Helper Responsibilities

The shared module should provide helpers with responsibilities equivalent to:

- `NormalizeMediaTitle(raw string) string`
- `BuildMediaQueryCandidates(raw string) []string`
- `TokenizeMediaMatchTerms(raw string) []string`
- `TitlesCompatible(query string, candidate string) bool`
- `ScoreTitleMatch(query string, candidate string) int`

The exact names can differ, but the separation of concerns should remain.

## Integration Plan

### `internal/subscription/resource_search.go`

Change result filtering so that:

- unsupported URLs are still discarded before matching;
- results must still contain at least one supported share link;
- title filtering uses shared title compatibility instead of raw normalized `contains`.

This should improve cases like:

- title is noisy but clearly the same media work;
- title contains both Chinese and English names;
- content mentions the keyword but title does not, which must still be filtered out.

### `internal/subscription/telegram.go`

Change `subscriptionEntryMatches` so it no longer relies only on compact normalized substring checks.

Instead:

- build a small set of query candidates from `sub.TMDBName` and `sub.Name`;
- build comparison text from entry name / path as today;
- treat the entry as matched when at least one meaningful candidate is compatible with at least one entry title/path representation.

This should improve temp-transfer matching for noisy or bilingual paths without making the matcher overly fuzzy.

### `internal/media/recognize/recognize.go`

Refactor `NormalizeTitle` and `BuildQueryCandidates` to use the shared logic or align closely with it.

Requirements:

- keep `recognize.Result` unchanged;
- preserve existing season / episode / year extraction behavior unless tests prove a needed fix;
- improve title cleanup quality using the new shared normalizer.

### `internal/media/tmdb/tmdb.go`

Keep the current scoring structure but strengthen it with the shared helpers.

Target behavior:

- run a compatibility gate before ranking weak candidates;
- use better tokenization for overlap scoring;
- keep existing year and media-type signals;
- prefer exact / strongly compatible titles over longer accidental substring matches.

The first version does not need a full ambiguity state machine, but it should avoid obvious false-positive wins when compatibility is weak.

### `drivers/139/share_risk_rename.go`

Adopt the shared normalization / candidate logic in the title resolution path where practical.

Target behavior:

- dirty Chinese / bilingual folder names produce better recognized titles;
- TMDB lookup has better query candidates;
- fallback rename decisions become more stable without changing the approved retry / rename workflow.

## Matching Rules

The first implementation should use these rules consistently:

1. Reject empty or generic-only titles.
2. Normalize both sides using the shared normalizer.
3. If normalized titles are equal, treat them as compatible.
4. Otherwise compare meaningful token sets:
   - CJK terms;
   - ASCII terms;
   - selected long-CJK segment candidates.
5. Require a meaningful overlap threshold instead of a raw `contains` check alone.
6. Allow compact substring checks only as a secondary signal after token compatibility.

This design intentionally prefers precision over recall for filtering scenarios like resource search.

## Testing Strategy

Add or extend tests in the following areas.

### Resource Search

Cover:

- mixed Chinese / English titles that should match after cleanup;
- title mismatch with content-only keyword mention, which must not match;
- unsupported URLs such as `t.me` or forum pages, which must not count as resource links;
- noisy titles containing release tokens that should still match once normalized.

### Recognize

Cover:

- English release-noise removal;
- Chinese release-noise removal;
- CJK / ASCII boundary splitting;
- preservation of pure-year titles;
- candidate generation for bilingual names and stripped prefixes.

### TMDB

Cover:

- exact title match;
- bilingual candidate overlap;
- longer accidental substring candidates losing to stronger token-compatible ones;
- year mismatch penalties still working after the new compatibility layer.

### 139 Share Risk Rename

Cover:

- dirty filenames producing better recognize / query candidates;
- rename planning still preserving season / episode and extension structure;
- existing retry / rename tests remaining green.

## Risks and Guardrails

- Over-aggressive normalization can collapse distinct titles into the same query.
  - Guardrail: add tests for known edge cases and keep the typo-fix set very small.
- Over-fuzzy matching can reintroduce noisy search results.
  - Guardrail: resource-search filtering remains strict and requires both title compatibility and a supported share link.
- Cross-module reuse can create hidden coupling.
  - Guardrail: keep the shared module small and side-effect free.

## Rollout Order

Recommended implementation order:

1. introduce shared normalization / tokenization helpers with unit tests;
2. switch `resource_search` to the new compatibility check;
3. align `recognize` candidate generation with the shared helpers;
4. strengthen TMDB candidate compatibility / scoring;
5. connect the improved title logic into 139 share-risk rename tests and implementation.

This order delivers immediate user-visible improvement in subscription search while reducing regression risk for TMDB and rename behavior.
