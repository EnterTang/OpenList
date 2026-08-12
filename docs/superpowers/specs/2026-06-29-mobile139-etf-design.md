# Mobile 139 ETF Design

## Goal

Implement Mobile 139 `.etf` support in OpenList with a PR-friendly scope:

- Generate `.etf` metadata after ordinary file upload.
- Optionally delete the uploaded source file after `.etf` generation.
- Restore the original file from a `.etf` file using Mobile 139 rapid create.
- Restore a temporary playback file from `.etf` and return a playable video link.
- Clean the recycle bin after deleting source, `.etf`, and temporary playback files.
- Store generated `.etf` files under an optional dedicated management folder.
- Build dedicated `.etf` folder paths from TMDB metadata and configurable secondary category rules.

The implementation should focus on the `139Yun` `personal_new` driver because the required rapid create API uses SHA256 metadata and the current driver already computes SHA256 during upload.

## Non-Goals

- Do not implement broad media-library management, batch archive moves, training datasets, or manual review workflows from `yidong139_pan_manager`.
- Do not add a new frontend application or a separate settings backend.
- Do not change unrelated drivers.
- Do not make ETF behavior default-on for existing users.

## References

- `GitYuA/OpenList-CAS`: driver-level CAS generation, restore, temporary playback, and a narrow core preview-name interface.
- `/Volumes/extend Disk/Gitlab/etflix_cloudpan`: ETF format, Mobile 139 rapid restore, TMDB metadata enrichment, and category-path generation.
- `/Volumes/extend Disk/Gitlab/pan139_fastlink`: ETF playback through temporary rapid-created files and configurable category rules.
- `/Volumes/extend Disk/Gitlab/yidong139_pan_manager`: media filename cleanup, query candidate generation, TMDB matching, and category YAML conventions.

## Data Formats

### ETF File

Use the existing ETF format from `etflix_cloudpan` and `pan139_fastlink`: one base64-encoded JSON record per line.

```json
{"name":"Movie.mkv","size":2147483648,"sha256":"<UPPERCASE_SHA256>","create_time":"2026-06-29T12:00:00+08:00"}
```

Only one record is required for OpenList upload-generated `.etf` files. The parser should accept multiple records for compatibility, but restore/playback should use the first valid record unless a later requirement adds multi-file ETF restore.

### Category YAML

Add a global setting named `media_category_rules` with type `text`. Its default value is the user-provided YAML:

```yaml
movie:
  动画片:
    genre_ids: '16'
  纪录片:
    genre_ids: '99'
  儿童家庭:
    genre_ids: '10751'
  动作片:
    genre_ids: '28'
  冒险片:
    genre_ids: '12'
  科幻片:
    genre_ids: '878'
  奇幻片:
    genre_ids: '14'
  悬疑片:
    genre_ids: '9648'
  惊悚片:
    genre_ids: '53'
  恐怖片:
    genre_ids: '27'
  犯罪片:
    genre_ids: '80'
  战争片:
    genre_ids: '10752'
  西部片:
    genre_ids: '37'
  喜剧片:
    genre_ids: '35'
  爱情片:
    genre_ids: '10749'
  剧情片:
    genre_ids: '18'
  历史片:
    genre_ids: '36'
  音乐片:
    genre_ids: '10402'
  电视电影:
    genre_ids: '10770'
  华语电影:
    original_language: 'zh,cn,tw,hk'
  外语电影:
    original_language: '!zh,!cn,!tw,!hk'
tv:
  动漫:
    genre_ids: '16'
  纪录片:
    genre_ids: '99'
  综艺:
    genre_ids: '10764,10767'
  儿童节目:
    genre_ids: '10762'
  国产剧:
    origin_country: 'CN'
    original_language: 'zh,cn'
  港台剧:
    origin_country: 'TW,HK'
  日韩剧:
    origin_country: 'JP,KR'
  欧美剧:
    origin_country: 'US,GB,CA,AU,FR,DE,IT,ES'
  海外其他剧:
    origin_country: '!CN,!TW,!HK,!JP,!KR,!US,!GB'
  未分类:
```

The parser must preserve YAML order because rule priority is top-to-bottom. A rule matches if any positive criterion matches and no negative criterion rejects it. Empty rules are fallback labels.

## Settings

Add global settings:

- `tmdb_api_key`: string, private.
- `tmdb_api_base_url`: string, private, default `https://api.themoviedb.org/3`.
- `tmdb_language`: string, private, default `zh-CN`.
- `media_category_rules`: text, private.

Add 139 driver settings:

- `generate_etf`: bool, default `false`.
- `delete_source_after_etf`: bool, default `false`.
- `restore_source_from_etf`: bool, default `false`.
- `delete_etf_after_restore`: bool, default `false`.
- `etf_download_restore`: bool, default `false`.
- `etf_video_playback`: bool, default `false`.
- `etf_root_folder_id`: string, optional dedicated ETF management root folder ID.
- `etf_root_path`: string, optional dedicated ETF management path to create/find under the mounted 139 storage.
- `etf_temp_folder_id`: string, optional playback temp folder ID.
- `etf_ext_allowlist`: string, optional comma-separated source extensions; empty means all non-ETF files.

The driver should reject ETF-specific operations with a clear error when `Type != personal_new`.

## Components

### `internal/etfmeta`

Responsibilities:

- Parse and generate ETF records.
- Validate file name, positive size, and 64-char SHA256.
- Normalize SHA256 to uppercase.
- Resolve restore names from `.etf` names and embedded `name`.
- Generate default `.etf` names when no TMDB metadata exists.

### `internal/media/category`

Responsibilities:

- Parse `media_category_rules` YAML using `yaml.Node`.
- Preserve rule order.
- Match `genre_ids`, `origin_country`, and `original_language`.
- Support negative tokens prefixed with `!`.
- Return fallback category for empty rules.

### `internal/media/recognize`

Responsibilities:

- Extract media candidates from uploaded file name and parent path.
- Clean release noise tokens based on the `yidong139_pan_manager` subset:
  `2160p`, `1080p`, `720p`, `4k`, `uhd`, `hdr`, `dv`, `dovi`, `hevc`, `x264`, `x265`, `bluray`, `web-dl`, `webrip`, `remux`, `atmos`, `truehd`, `dts`, `aac`, `flac`, `mkv`, `mp4`, `proper`, `imax`, `nf`, `netflix`, `amzn`, `hulu`, Chinese audio/subtitle/release words, export timestamps, and size suffixes.
- Extract explicit TMDB markers such as `{tmdbid-123}` and `{tmdb-123}`.
- Extract year hints without treating pure-year titles like `1917` and `1923` as release years.
- Extract season/episode from `S01E02`, `Season 2`, `第 2 季`, `第 3 集`, `EP03`, and common numbered episode stems.
- Prefer parent-folder query candidates for episode-like files when the file stem is generic.

### `internal/media/tmdb`

Responsibilities:

- Read TMDB settings through `internal/setting`.
- Search TMDB using configured API key, base URL, and language.
- Fetch details by explicit TMDB ID.
- Score candidates using title similarity, media-type hint, year proximity, and popularity.
- Return structured metadata:
  `media_type`, `tmdb_id`, `name`, `year`, `genre_ids`, `origin_country`, `original_language`.
- Return no metadata on low confidence, missing API key, or network failure. ETF generation must still succeed with fallback path generation.

### `drivers/139`

Responsibilities:

- Generate ETF after successful `personal_new` upload using already computed SHA256.
- Restore files from ETF by calling `/file/create` with `contentHashAlgorithm: SHA256` and ETF size/hash.
- For playback, create a temporary real file under the configured temp folder, get a download URL, then delete the temp file and clear recycle bin.
- When deleting source, ETF, or temp files as part of ETF workflows, call recycle-bin cleanup after successful trash operations.
- Keep UA and client info aligned with Mobile Cloud PC/macOS client behavior already introduced for 500G upload support.

## Flows

### Upload and ETF Generation

1. `Put` receives the uploaded stream.
2. Driver computes or reuses SHA256.
3. Driver uploads or rapid-creates the source file.
4. If `generate_etf` is off, return the normal upload result.
5. Build ETF record from uploaded file name, size, SHA256, and create time.
6. Run media recognition using source file name and destination parent path.
7. If TMDB metadata resolves, calculate secondary category from `media_category_rules`.
8. Resolve ETF destination:
   - dedicated root if configured;
   - otherwise same upload directory.
9. Upload the ETF text file.
10. If `delete_source_after_etf` is on, trash the uploaded source file and clear recycle bin.

### Restore From ETF

1. User uploads or copies a `.etf` file while `restore_source_from_etf` is enabled, or invokes a restore action through existing write flow.
2. Driver reads ETF content.
3. Driver calls Mobile 139 `/file/create` with ETF SHA256 and size.
4. If rapid creation requires body upload URLs, return an explicit "rapid restore unavailable" error.
5. Optionally delete the `.etf` and clear recycle bin when `delete_etf_after_restore` is enabled.

### Video Playback From ETF

1. OpenList requests `/d` or `/p` for a `.etf` file with video preview/download context.
2. A narrow core interface similar to OpenList-CAS returns the preview file name from ETF metadata so OpenList treats it as the original video type.
3. Driver `Link` handles a dedicated ETF playback link type.
4. Driver creates a temp folder/file via rapid create.
5. Driver obtains a normal Mobile 139 download URL.
6. Driver schedules deletion of the temp file/folder and clears recycle bin.

## Error Handling

- ETF generation failure should fail the upload only when the source file cannot be verified or the ETF destination cannot be created. TMDB lookup failure should not fail upload.
- TMDB errors should be logged with API keys redacted.
- Invalid YAML should make category matching fall back to built-in defaults while preserving the raw setting for user correction.
- Rapid restore should fail clearly when cloud data is unavailable and `/file/create` returns upload URLs.
- Temp playback cleanup errors should be logged but should not invalidate an already returned download URL.

## Tests

Add focused Go tests for:

- ETF encode/decode and SHA256 normalization.
- Category YAML positive, negative, fallback, and order-priority matching.
- Media recognition cases from `yidong139_pan_manager`:
  - `Cars.3.2017.2160p.BluRay.REMUX.HEVC...mkv` -> `Cars 3`, year 2017.
  - `The.Witcher.S03E04.2023.2160p.NF.WEB-DL...mkv` -> title `The Witcher`, season 3, episode 4.
  - `嗜血法医 第8季 豆瓣8.8` -> season 8.
  - `1917` stays title `1917` with no year hint.
  - `6.大黄蜂 4K原盘REMUX 杜比视界 国英双音` -> query includes `大黄蜂`.
- TMDB scoring with fake HTTP responses.
- 139 rapid restore payload uses SHA256 and rejects returned upload URLs.
- 139 playback cleanup calls delete and recycle cleanup after getting download URL.
- Existing 500G upload tests continue to pass.

## Risks

- TMDB matching can misidentify ambiguous titles. The design limits damage by falling back when confidence is low and by not renaming source files during ETF generation.
- OpenList's bundled frontend is not present in this repository. The first PR can expose YAML editing through the existing text setting item; a richer YAML editor requires the corresponding frontend repository or generated static asset workflow.
- ETF playback needs a small core hook to expose the original media type for `.etf` files. This mirrors OpenList-CAS and should stay generic, narrow, and driver opt-in.
