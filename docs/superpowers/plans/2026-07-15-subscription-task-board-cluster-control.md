# Subscription Task Board and Cluster Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show subscription work by real content changes, expose each episode's selected source and worker, and consolidate home cluster controls behind a role-aware parent page.

**Architecture:** Persist the selected source in a per-episode projection when transfer work is successfully created; do not infer it from mutable subscription-item timestamps. Build the task-board list, totals, and failure queue from one shared database filter, then compose existing cluster views inside a role-aware SolidJS home container.

**Tech Stack:** Go, GORM, Gin, SQLite-backed Go tests, SolidJS, Hope UI, TypeScript, pnpm/Vite.

---

## File Structure

### Backend: /Volumes/extend Disk/Github/OpenList

- Modify: internal/model/subscription.go - episode-source persistence and API response fields.
- Modify: internal/db/db.go - automatic migration.
- Modify: internal/db/subscription.go - snapshot CRUD, run filters, board totals, worker detail query.
- Modify: internal/subscription/transfer_task.go and internal/subscription/telegram.go - standalone snapshot timing.
- Modify: internal/subscription/cluster_dispatch.go - cluster snapshot timing.
- Modify: server/handles/subscription.go and server/router.go - validated board/details APIs.
- Test: internal/db/subscription_test.go, internal/subscription/transfer_task_test.go, internal/subscription/cluster_dispatch_test.go, server/handles/subscription_test.go.

### Frontend: /Volumes/extend Disk/Github/OpenList-Frontend

- Modify: src/types/subscription.ts and src/utils/api.ts - typed contracts.
- Modify: src/pages/manage/subscription/TransferTasks.tsx - server-filtered change board and failure pill.
- Modify: src/pages/home/SubscriptionManagement.tsx - source detail modal.
- Create: src/pages/home/ClusterControl.tsx - role-aware horizontal navigation.
- Modify: src/pages/home/HomeAppSidebar.tsx and src/pages/home/Layout.tsx - one home cluster parent route.
- Modify: src/pages/manage/cluster/Overview.tsx, Nodes.tsx, Jobs.tsx, Settings.tsx - embedded rendering mode.
- Modify: src/lang/en/home.json, src/lang-overrides/zh-CN/home.json, src/lang-overrides/zh-TW/home.json, and the matching subscription JSON files.
- Modify generated output: /Volumes/extend Disk/Github/OpenList/public/dist/**.

### Concurrent changes

The listed backend and frontend source files already contain unrelated, uncommitted progress/TMDB changes. Preserve those hunks. When a commit is requested, stage shared files with git add -p and verify git diff --cached before committing.

### Task 1: Add the current episode-source projection

**Files:**
- Modify: internal/model/subscription.go
- Modify: internal/db/db.go
- Modify: internal/db/subscription.go
- Test: internal/db/subscription_test.go

- [ ] **Step 1: Write the failing uniqueness and delete-cascade tests.**

~~~go
func TestUpsertSubscriptionEpisodeSourceReplacesSameEpisode(t *testing.T) {
	setupETFArchiveDB(t)
	first, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: 7, Season: 1, Episode: 2, SubscriptionItemID: 10,
		SourceType: model.SubscriptionSourceTelegram, SourceProvider: "quark",
		ShareURL: "https://pan.quark.cn/s/old", FileName: "Show.S01E02.1080p.mkv",
		SelectedAt: time.Unix(100, 0),
	})
	if err != nil { t.Fatal(err) }
	second, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: 7, Season: 1, Episode: 2, SubscriptionItemID: 11,
		SourceType: model.SubscriptionSourcePanSou, SourceProvider: "pan123",
		ShareURL: "https://www.123pan.com/s/new", FileName: "Show.S01E02.2160p.mkv",
		ClusterJobID: "job-11", SelectedAt: time.Unix(200, 0),
	})
	if err != nil { t.Fatal(err) }
	if first.ID != second.ID || second.SubscriptionItemID != 11 || second.SourceProvider != "pan123" {
		t.Fatalf("episode source = %#v, want replacement", second)
	}
}

func TestDeleteSubscriptionDeletesEpisodeSources(t *testing.T) {
	setupETFArchiveDB(t)
	sub := &model.Subscription{Name: "delete"}
	if err := CreateSubscription(sub); err != nil { t.Fatal(err) }
	if _, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID, FileName: "movie.mkv", SelectedAt: time.Now(),
	}); err != nil { t.Fatal(err) }
	if err := DeleteSubscription(sub.ID); err != nil { t.Fatal(err) }
	sources, err := ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sources) != 0 { t.Fatalf("sources=%#v err=%v", sources, err) }
}
~~~

- [ ] **Step 2: Run the tests to confirm they fail before the feature exists.**

Run: go test ./internal/db -run 'Test(UpsertSubscriptionEpisodeSourceReplacesSameEpisode|DeleteSubscriptionDeletesEpisodeSources)' -count=1

Expected: compile failure for SubscriptionEpisodeSource and its data-access functions.

- [ ] **Step 3: Add the model and GORM upsert contract.**

~~~go
type SubscriptionEpisodeSource struct {
	ID                 uint      `json:"id" gorm:"primarykey"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	SubscriptionID     uint      `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_episode_source_slot;index"`
	Season             int       `json:"season" gorm:"uniqueIndex:idx_subscription_episode_source_slot"`
	Episode            int       `json:"episode" gorm:"uniqueIndex:idx_subscription_episode_source_slot"`
	SubscriptionItemID uint      `json:"subscription_item_id" gorm:"index"`
	SourceType         string    `json:"source_type" gorm:"index"`
	SourceProvider     string    `json:"source_provider" gorm:"index"`
	ShareURL           string    `json:"share_url" gorm:"type:text"`
	FileName           string    `json:"file_name"`
	ClusterJobID       string    `json:"cluster_job_id" gorm:"size:64;index"`
	SelectedAt         time.Time `json:"selected_at" gorm:"index"`
}

func UpsertSubscriptionEpisodeSource(source *model.SubscriptionEpisodeSource) (*model.SubscriptionEpisodeSource, error) {
	if source == nil { return nil, errors.New("subscription episode source is nil") }
	if source.SelectedAt.IsZero() { source.SelectedAt = time.Now() }
	err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "subscription_id"}, {Name: "season"}, {Name: "episode"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"subscription_item_id", "source_type", "source_provider", "share_url",
			"file_name", "cluster_job_id", "selected_at", "updated_at",
		}),
	}).Create(source).Error
	if err != nil { return nil, errors.WithStack(err) }
	var saved model.SubscriptionEpisodeSource
	err = db.Where("subscription_id = ? AND season = ? AND episode = ?",
		source.SubscriptionID, source.Season, source.Episode).First(&saved).Error
	return &saved, errors.WithStack(err)
}
~~~

Register new(model.SubscriptionEpisodeSource) immediately after new(model.SubscriptionItem) in db.Init. Add ListSubscriptionEpisodeSources ordered by season, episode, selected_at; delete snapshot rows inside DeleteSubscription before deleting the subscription.

- [ ] **Step 4: Run the focused database suite.**

Run: go test ./internal/db -run 'Subscription(EpisodeSource|Run)|TestDeleteSubscription' -count=1

Expected: PASS.

### Task 2: Capture sources only when transfer work is successfully created

**Files:**
- Modify: internal/subscription/transfer_task.go
- Modify: internal/subscription/telegram.go
- Modify: internal/subscription/cluster_dispatch.go
- Test: internal/subscription/transfer_task_test.go
- Test: internal/subscription/cluster_dispatch_test.go
- Test: internal/subscription/standalone_target_integration_test.go

- [ ] **Step 1: Write failing standalone and cluster timing tests.**

~~~go
func TestApplyItemTransferStoresSourceAfterTaskAccepted(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{ID: 71, SourceType: model.SubscriptionSourceTelegram}
	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "tg:71", SourceProvider: "quark",
		SourceURL: "https://pan.quark.cn/s/71", FileName: "Show.S01E01.mkv",
		Season: 1, Episode: 1, TargetDir: "/delivery", Status: model.SubscriptionItemStatusPending,
	})
	if err != nil { t.Fatal(err) }
	oldTransfer := transferItem
	transferItem = func(context.Context, *model.SubscriptionItem, bool) error { return nil }
	t.Cleanup(func() { transferItem = oldTransfer })
	if _, count, err := applyItemTransfer(context.Background(), sub, item, false); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sources) != 1 || sources[0].ClusterJobID != "" {
		t.Fatalf("sources=%#v err=%v", sources, err)
	}
}
~~~

Add a second test derived from TestClusterDispatchFailureMarksItemFailed: its dispatcher returns no worker, dispatchClusterItems returns an error, and ListSubscriptionEpisodeSources returns no rows. Extend TestClusterDispatchPersistsContextAndTransitionsStatus to assert one snapshot containing the returned job id.

- [ ] **Step 2: Run the lifecycle tests.**

Run: go test ./internal/subscription -run 'Test(ApplyItemTransferStoresSourceAfterTaskAccepted|ClusterDispatch)' -count=1

Expected: compile failure because applyItemTransfer has no subscription argument and snapshots are never written.

- [ ] **Step 3: Implement the two accepted-work boundaries.**

~~~go
func snapshotSelectedEpisodeSource(sub *model.Subscription, item *model.SubscriptionItem, jobID string) error {
	if sub == nil || item == nil { return errors.New("subscription and item are required") }
	_, err := db.UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID, Season: item.Season, Episode: item.Episode,
		SubscriptionItemID: item.ID, SourceType: sub.SourceType,
		SourceProvider: item.SourceProvider, ShareURL: item.SourceURL,
		FileName: item.FileName, ClusterJobID: strings.TrimSpace(jobID), SelectedAt: time.Now(),
	})
	return err
}
~~~

Change applyItemTransfer and the injected applySubscriptionItemTransfer hook to receive sub *model.Subscription. Call snapshotSelectedEpisodeSource only after transferItem succeeds and before returning a count of one. Update all test injection signatures.

In dispatchClusterItems, call the same helper only in the branch where result.Error is nil and result.JobID is non-empty. Do not write snapshots from CompleteClusterTransfer, FailClusterTransfer, markSubscriptionTransferSucceeded, or markSubscriptionTransferFailed.

- [ ] **Step 4: Run all subscription tests.**

Run: go test ./internal/subscription -count=1

Expected: PASS, including standalone target integration tests.

### Task 3: Add one shared database filter for changes, failures, and board totals

**Files:**
- Modify: internal/model/subscription.go
- Modify: internal/db/subscription.go
- Test: internal/db/subscription_test.go

- [ ] **Step 1: Write the failing query/total contract test.**

~~~go
func TestSubscriptionBoardUsesSameFiltersForRowsAndTotals(t *testing.T) {
	setupETFArchiveDB(t)
	first := &model.Subscription{Name: "Alpha", SourceType: model.SubscriptionSourceTelegram}
	second := &model.Subscription{Name: "Beta", SourceType: model.SubscriptionSourcePanSou}
	if err := CreateSubscription(first); err != nil { t.Fatal(err) }
	if err := CreateSubscription(second); err != nil { t.Fatal(err) }
	runs := []model.SubscriptionRun{
		{SubscriptionID: first.ID, StartedAt: time.Now().Add(-3*time.Minute), Status: model.SubscriptionStatusSuccess, AddedCount: 2},
		{SubscriptionID: first.ID, StartedAt: time.Now().Add(-2*time.Minute), Status: model.SubscriptionStatusSuccess, TransferredCount: 9},
		{SubscriptionID: first.ID, StartedAt: time.Now().Add(-time.Minute), Status: model.SubscriptionStatusFailed, Error: "timeout"},
		{SubscriptionID: second.ID, StartedAt: time.Now(), Status: model.SubscriptionStatusSuccess, ChangedCount: 1},
	}
	for i := range runs { if err := CreateSubscriptionRun(&runs[i]); err != nil { t.Fatal(err) } }
	filter := SubscriptionRunFilter{SubscriptionID: first.ID, View: SubscriptionRunViewChanges, Page: 1, PerPage: 20}
	items, total, err := ListSubscriptionRuns(filter)
	if err != nil || total != 1 || len(items) != 1 || items[0].AddedCount != 2 {
		t.Fatalf("changes=%#v total=%d err=%v", items, total, err)
	}
	board, err := GetSubscriptionBoard(filter)
	if err != nil || board.AddedCount != 2 || board.ChangedRunCount != 1 || board.FailureCount != 1 {
		t.Fatalf("board=%#v err=%v", board, err)
	}
}
~~~

- [ ] **Step 2: Run the test to establish the failure.**

Run: go test ./internal/db -run TestSubscriptionBoardUsesSameFiltersForRowsAndTotals -count=1

Expected: compile failure for View, SubscriptionRunViewChanges, and GetSubscriptionBoard.

- [ ] **Step 3: Implement the run projection, views, and totals.**

~~~go
const (
	SubscriptionRunViewChanges = "changes"
	SubscriptionRunViewFailures = "failures"
)

func applySubscriptionRunView(query *gorm.DB, view string) *gorm.DB {
	switch view {
	case SubscriptionRunViewChanges:
		return query.Where("subscription_runs.status = ? AND (subscription_runs.added_count > 0 OR subscription_runs.changed_count > 0)", model.SubscriptionStatusSuccess)
	case SubscriptionRunViewFailures:
		return query.Where("subscription_runs.status = ? OR subscription_runs.error <> ?", model.SubscriptionStatusFailed, "")
	default:
		return query.Where(meaningfulSubscriptionRunCondition(), model.SubscriptionStatusSuccess, "")
	}
}
~~~

Extend SubscriptionRunFilter with View, Keyword, and SourceType. Make subscriptionRunQuery join subscriptions as subscription and apply subscription_id, keyword, source_type, status, and then applySubscriptionRunView. Select subscription.name AS subscription_name and subscription.source_type AS subscription_source_type into non-persisted fields on model.SubscriptionRun.

Implement GetSubscriptionBoard(filter) using the same subscriptionRunQuery predicate before pagination: filtered subscription count, change-run count, SUM(added_count), SUM(changed_count), and failure count. ClearFailedSubscriptionRuns must delete with applySubscriptionRunView(..., SubscriptionRunViewFailures), never successful change rows.

- [ ] **Step 4: Run database regressions.**

Run: go test ./internal/db -count=1

Expected: PASS. A pure successful transfer with only TransferredCount remains persisted when applicable but is absent from the changes view.

### Task 4: Expose validated board and episode-source detail APIs

**Files:**
- Modify: internal/db/subscription.go
- Modify: server/handles/subscription.go
- Modify: server/handles/subscription_test.go
- Modify: server/router.go

- [ ] **Step 1: Write handler-level validation and worker-name tests.**

~~~go
func TestValidateSubscriptionRunView(t *testing.T) {
	for _, value := range []string{"", "changes", "failures"} {
		if err := validateSubscriptionRunView(value); err != nil { t.Fatalf("view %q: %v", value, err) }
	}
	if err := validateSubscriptionRunView("all"); err == nil { t.Fatal("invalid view accepted") }
}

func TestEpisodeSourceWorkerNamePrefersAssignmentThenAttempt(t *testing.T) {
	if got := episodeSourceWorkerName("Coordinator A", "Retry worker"); got != "Coordinator A" { t.Fatalf("worker=%q", got) }
	if got := episodeSourceWorkerName("", "Retry worker"); got != "Retry worker" { t.Fatalf("worker=%q", got) }
	if got := episodeSourceWorkerName("", ""); got != "未指派" { t.Fatalf("worker=%q", got) }
}
~~~

- [ ] **Step 2: Run the focused tests.**

Run: go test ./server/handles -run 'Test(ValidateSubscriptionRunView|EpisodeSourceWorkerNamePrefersAssignmentThenAttempt)' -count=1

Expected: compile failure for the helpers.

- [ ] **Step 3: Add request/response handling and the worker query.**

Add view, source_type, and keyword to listSubscriptionRunsReq. Validate view before calling db.ListSubscriptionRuns.

Define SubscriptionEpisodeSourceDetail with status, source fields, worker_name, and selected_at. Implement ListSubscriptionEpisodeSourceDetails(subscriptionID) by joining snapshots to subscription items, cluster_jobs, assigned ClusterNode, latest ClusterJobAttempt, and fallback ClusterNode. Worker precedence: assigned node name, latest attempt node name, then 未指派. A snapshot without ClusterJobID returns 本机.

~~~go
func GetSubscriptionBoard(c *gin.Context) {
	filter, err := subscriptionRunFilterFromRequest(c)
	if err != nil { common.ErrorResp(c, err, 400); return }
	value, err := db.GetSubscriptionBoard(filter)
	if err != nil { common.ErrorResp(c, err, 500); return }
	common.SuccessResp(c, value)
}

func ListSubscriptionEpisodeSources(c *gin.Context) {
	id, err := parseRequiredSubscriptionID(c.Query("subscription_id"))
	if err != nil { common.ErrorResp(c, err, 400); return }
	items, err := db.ListSubscriptionEpisodeSourceDetails(id)
	if err != nil { common.ErrorResp(c, err, 500); return }
	common.SuccessResp(c, gin.H{"content": items})
}
~~~

Register GET /admin/subscription/board and GET /admin/subscription/episode_sources beside GET /admin/subscription/runs.

- [ ] **Step 4: Verify handlers and compilation.**

Run: go test ./internal/db ./internal/subscription ./server/handles -count=1 && go test ./server -run '^$' -count=1

Expected: PASS.

### Task 5: Add the typed frontend contracts and translations

**Files:**
- Modify: src/types/subscription.ts
- Modify: src/utils/api.ts
- Modify: src/lang/en/subscription.json
- Modify: src/lang-overrides/zh-CN/subscription.json
- Modify: src/lang-overrides/zh-TW/subscription.json
- Modify: src/lang/en/home.json
- Modify: src/lang-overrides/zh-CN/home.json
- Modify: src/lang-overrides/zh-TW/home.json

- [ ] **Step 1: Add the client interfaces.**

~~~ts
export type SubscriptionRunView = "changes" | "failures"

export interface SubscriptionBoard {
  subscription_count: number
  changed_run_count: number
  added_count: number
  changed_count: number
  failure_count: number
}

export interface SubscriptionEpisodeSource {
  id: number
  subscription_id: number
  season: number
  episode: number
  status: string
  source_type: SubscriptionSourceType
  source_provider: string
  share_url: string
  file_name: string
  cluster_job_id: string
  worker_name: string
  selected_at: string
}
~~~

Extend SubscriptionRun with subscription_name and subscription_source_type. Keep SubscriptionDetail and SubscriptionItem because active creation/progress components still consume them.

- [ ] **Step 2: Add typed requests with server parameter names.**

~~~ts
export interface SubscriptionRunQuery {
  subscription_id?: number
  view?: SubscriptionRunView
  status?: string
  source_type?: string
  keyword?: string
  page?: number
  per_page?: number
}

export const subscriptionBoard = (params: SubscriptionRunQuery = {}): PResp<SubscriptionBoard> =>
  r.get("/admin/subscription/board", { params })

export const subscriptionEpisodeSources = (subscriptionID: number): PResp<{ content: SubscriptionEpisodeSource[] }> =>
  r.get("/admin/subscription/episode_sources", { params: { subscription_id: subscriptionID } })
~~~

Change subscriptionRuns to accept SubscriptionRunQuery.

- [ ] **Step 3: Add UI copy in all three locales.**

Add subscription keys: board_added_files, board_changed_runs, board_subscription_filter, board_change_records, board_failure_pill, board_show_failures, board_clear_failures, view_details, episode_source_title, episode_source_empty, source_origin, source_provider, source_file, assigned_worker, worker_local, worker_unassigned. Add home.sidebar.cluster_control.

Display TG, PS, 123, 115, Quark, and Ali in the components, not as data-dependent translation keys.

- [ ] **Step 4: Typecheck contracts before UI work.**

Run: pnpm lint

Expected: PASS.

### Task 6: Rebuild the task board and failure notification

**Files:**
- Modify: src/pages/manage/subscription/TransferTasks.tsx
- Test: browser verification at http://localhost:63627/

- [ ] **Step 1: Delete the board's embedded subscription source browser.**

Remove SubscriptionItem, subscriptionGet, selectedSubscriptionID, episodeItems, toggleEpisodeSources, episodeLabel, episodeProviderLabel, the subscriptions table, and the expanded episode-source panel. Details move exclusively to SubscriptionManagement.

- [ ] **Step 2: Replace local aggregation with the shared server query.**

~~~ts
const query = createMemo<SubscriptionRunQuery>(() => ({
  subscription_id: selectedSubscriptionID(),
  source_type: sourceFilter() === "all" ? undefined : sourceFilter(),
  keyword: keyword().trim() || undefined,
}))

const refresh = async () => {
  const [subscriptionsResp, boardResp, changesResp, failuresResp] = await Promise.all([
    subscriptionList({ page: 1, per_page: 0 }),
    subscriptionBoard(query()),
    subscriptionRuns({ ...query(), view: "changes", page: 1, per_page: 30 }),
    subscriptionRuns({ ...query(), view: "failures", page: 1, per_page: 6 }),
  ])
  handleResp(subscriptionsResp, (data) => setSubscriptions(data.content || []))
  handleResp(boardResp, setBoard)
  handleResp(changesResp, (data) => setRuns(data.content || []))
  handleResp(failuresResp, (data) => setFailedRuns(data.content || []))
}
~~~

Use a Hope UI Select with an all-subscriptions option. The user selects a subscription, source filter, or keyword and refreshes; debounce keyword application by 250ms. The main table displays only changes results and uses subscription_name supplied by the server.

- [ ] **Step 3: Render correct totals and an expandable lower-right failure pill.**

Replace the transferred metric with board().added_count and label it 新增文件. Use board().changed_count and board().changed_run_count for change metrics. Use board().subscription_count instead of the client-side loaded page count.

Show a fixed bottom-right Box only when failures exist. Its closed trigger shows only the count and an aria-label for opening failures. On click, open a Hope UI Popover on desktop or Drawer on mobile with failure summaries and the clear button. Do not show 查看 or 清除 text on the closed pill. After clearing, call refresh and hide the pill when no failures remain.

- [ ] **Step 4: Validate the board.**

Run: pnpm lint && pnpm build

Expected: PASS.

Browser checks:
1. Changing subscription updates metrics, changes list, and failure count together.
2. A success record with only transferred_count is absent from recent execution records.
3. The lower-right closed pill has no action text; clear is available only after opening it.

### Task 7: Add per-card current-source details

**Files:**
- Modify: src/pages/home/SubscriptionManagement.tsx
- Modify: src/types/subscription.ts
- Modify: src/utils/api.ts
- Test: browser verification at http://localhost:63627/

- [ ] **Step 1: Add modal state and load sources on click.**

~~~ts
const details = createDisclosure()
const [detailSubscription, setDetailSubscription] = createSignal<Subscription>()
const [episodeSources, setEpisodeSources] = createSignal<SubscriptionEpisodeSource[]>([])
const [detailLoading, loadEpisodeSources] = useFetch(subscriptionEpisodeSources)

const openDetails = async (record: Subscription) => {
  setDetailSubscription(record)
  setEpisodeSources([])
  details.onOpen()
  const response = await loadEpisodeSources(record.id)
  handleResp(response, (data) => setEpisodeSources(data.content || []))
}
~~~

Pass openDetails to SubscriptionList and add a size=sm 查看详情 button on every card without changing existing check, transfer, edit, and delete actions.

- [ ] **Step 2: Render sources by season with only confirmed display fields.**

~~~ts
const sourceOriginLabel = (value: SubscriptionSourceType) =>
  value === "telegram" ? "TG" : value === "pansou" ? "PS" : t("subscription.source_manual")

const sourceProviderLabel = (value: string) =>
  ({ pan123: "123", pan115: "115", quark: "Quark", aliyun_drive: "Ali" })[
    value.trim().toLowerCase() as "pan123" | "pan115" | "quark" | "aliyun_drive"
  ] || value || "-"
~~~

Use a Hope UI Modal and compact table, grouped under Season headings. Rows show status, source origin, source disk, a new-tab filename anchor using share_url, worker_name, and selected_at. Do not display Telegram message content, Season 1 in source text, S01E01 as a filename substitute, 小芳, or 第 2 集. Empty data displays 暂未创建转存任务; no historical reconstruction occurs.

- [ ] **Step 3: Validate the modal.**

Run: pnpm lint && pnpm build

Expected: PASS.

Browser checks:
1. Each subscription card has 查看详情.
2. Filename is the direct share hyperlink and opens a new tab.
3. Worker labels are 本机 for standalone, assigned node name for jobs, and 未指派 for unassigned jobs.

### Task 8: Add one role-aware Cluster Control entry

**Files:**
- Create: src/pages/home/ClusterControl.tsx
- Modify: src/pages/home/HomeAppSidebar.tsx
- Modify: src/pages/home/Layout.tsx
- Modify: src/pages/manage/cluster/Overview.tsx
- Modify: src/pages/manage/cluster/Nodes.tsx
- Modify: src/pages/manage/cluster/Jobs.tsx
- Modify: src/pages/manage/cluster/Settings.tsx
- Test: browser verification at http://localhost:63627/

- [ ] **Step 1: Make each existing cluster page safe to embed.**

Each component accepts embedded?: boolean. Because Solid hooks cannot be conditional, create a child ClusterPageTitle component containing useManageTitle and render that child only when not embedded. Wrap existing PageHeader in Show when={!props.embedded()}. Do not change polling, API requests, management routes, forms, or actions.

- [ ] **Step 2: Create the role-aware container.**

~~~ts
type ClusterControlTab = "overview" | "nodes" | "tasks" | "settings"

const tabsForRole = (role: ClusterRole): ClusterControlTab[] =>
  role === "hybrid" || role === "coordinator"
    ? ["overview", "nodes", "tasks", "settings"]
    : ["tasks", "settings"]

const initialTab = (tabs: ClusterControlTab[]) => {
  const stored = localStorage.getItem("home_cluster_control_tab") as ClusterControlTab | null
  return stored && tabs.includes(stored) ? stored : tabs[0]
}
~~~

Load clusterGetConfig on mount. While loading show a compact loading state. On request failure, use the standalone/worker-safe tabs: tasks and settings. Correct an unavailable stored tab to the first current tab. Render an HStack of horizontal tab buttons; write selected values to home_cluster_control_tab; mount Overview, Nodes, TransferTasks, or Settings with embedded mode.

- [ ] **Step 3: Replace the sidebar task board with the parent Cluster Control entry.**

~~~ts
export type HomePageKey =
  | "netdisk"
  | "subscriptions"
  | "mobile_share"
  | "resource_search"
  | "cluster_control"

const initialHomePage = (): HomePageKey => {
  const stored = localStorage.getItem("home_app_page")
  if (stored === "task_board") return "cluster_control"
  // retain all current valid page keys, including cluster_control
  return "netdisk"
}
~~~

Remove the task_board page item. Add exactly one cluster_control item with the current cluster/network icon. Add the Layout Match that renders ClusterControl. Do not put role child pages in the sidebar.

- [ ] **Step 4: Validate roles and responsive layout.**

Run: pnpm lint && pnpm build

Expected: PASS.

Browser checks:
1. Sidebar contains one 集群控制 item, not a standalone 任务看板 item.
2. hybrid and coordinator show 总览, Worker 节点, 任务看板, 集群配置.
3. worker and standalone show only 任务看板 and 集群配置.
4. A stale overview selection falls back to task board for worker/standalone.
5. At 390px wide, the horizontal nav wraps or scrolls without covering content.

### Task 9: Synchronize assets and verify the complete change

**Files:**
- Modify generated output: /Volumes/extend Disk/Github/OpenList/public/dist/**

- [ ] **Step 1: Format and run backend checks.**

Run:
~~~bash
gofmt -w internal/model/subscription.go internal/db/db.go internal/db/subscription.go internal/db/subscription_test.go internal/subscription/transfer_task.go internal/subscription/telegram.go internal/subscription/cluster_dispatch.go internal/subscription/transfer_task_test.go internal/subscription/cluster_dispatch_test.go server/handles/subscription.go server/handles/subscription_test.go server/router.go
go test ./internal/db ./internal/subscription ./server/handles -count=1
go test ./internal/cluster/... -count=1
~~~

Expected: every command exits 0.

- [ ] **Step 2: Build frontend and synchronize the verified result.**

Run:
~~~bash
pnpm lint
pnpm build
rsync -a --delete dist/ '/Volumes/extend Disk/Github/OpenList/public/dist/'
git -C '/Volumes/extend Disk/Github/OpenList' diff --check
~~~

Expected: typecheck and Vite build succeed before rsync runs.

- [ ] **Step 3: Run full backend compile validation and final diff checks.**

Run:
~~~bash
go test ./... -run '^$' -count=1
git -C '/Volumes/extend Disk/Github/OpenList' status --short
git -C '/Volumes/extend Disk/Github/OpenList' diff --check
git -C '/Volumes/extend Disk/Github/OpenList-Frontend' status --short
git -C '/Volumes/extend Disk/Github/OpenList-Frontend' diff --check
~~~

Expected: compile succeeds and neither repository has whitespace errors. Separate pre-existing concurrent hunks from this feature; do not reset, revert, or accidentally stage them.

## Plan Review

**Spec coverage:** Tasks 1-2 implement source snapshot timing and last-selection replacement; Tasks 3-4 implement real change statistics, changes-only execution history, filters, failures, and worker lookup; Tasks 5-7 implement the board, modal, direct filename link, and role-specific parent navigation; Task 9 builds and synchronizes deployable assets.

**Placeholder scan:** Every code or behavior step names exact files, interfaces, query semantics, commands, and expected result. No later task relies on an undefined endpoint or data type.

**Type consistency:** SubscriptionEpisodeSource, SubscriptionRunView, SubscriptionRunQuery, SubscriptionBoard, and SubscriptionEpisodeSourceDetail are established before their request and UI consumers.
