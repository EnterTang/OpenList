package worker

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestListMoviePilotTaskStatusesShowsActiveAndRememberedSubscriptions(t *testing.T) {
	service := New(nil, nil)
	service.controlNodeID = "worker-1"
	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	activeTorrent := &protocol.TorrentTaskContext{
		BindingID: "binding-1", BridgeInstanceID: "mp-main", WorkerNodeID: "worker-1",
		Downloader: "qb-main", QBClientID: "qb-main", TorrentHash: hash,
	}
	service.active["job-1"] = &activeTask{
		offer: protocol.JobOffer{AttemptRef: protocol.AttemptRef{JobID: "job-1"}, TaskContext: protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{SubscriptionID: 21, SubscriptionItemID: 22, SubscriptionName: "赌王之王"},
			Media:        protocol.MediaTaskContext{LogicalTargetPath: "/剧集/赌王之王/S01E01.mkv"},
			Torrent:      activeTorrent,
		}},
		stage: model.ClusterStageQBObserving, stageStatus: model.ClusterStageStatusRunning,
		completedBytes: 25, totalBytes: 100, progressMessage: "downloading", progressAt: time.Now().UTC(),
	}
	rememberedTorrent := *activeTorrent
	rememberedTorrent.BindingID = "binding-2"
	rememberedTorrent.TorrentHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(&rememberedTorrent)] = moviePilotTorrentRegistryEntry{
		Torrent:      rememberedTorrent,
		Subscription: protocol.SubscriptionTaskContext{SubscriptionID: 30, SubscriptionItemID: 31, SubscriptionName: "草间弥生的生活"},
	}

	statuses := service.ListMoviePilotTaskStatuses()
	if len(statuses) != 2 {
		t.Fatalf("got %d statuses: %#v", len(statuses), statuses)
	}
	if statuses[0].Phase != model.MoviePilotTaskPhaseDownloading || statuses[0].DownloadProgress != .25 || statuses[0].SubscriptionName != "赌王之王" {
		t.Fatalf("active status = %#v", statuses[0])
	}
	if statuses[1].Phase != model.MoviePilotTaskPhaseBound || statuses[1].SubscriptionName != "草间弥生的生活" {
		t.Fatalf("remembered status = %#v", statuses[1])
	}
}

func TestSyncMoviePilotTorrentStatusKeepsSemanticWorkerLifecycle(t *testing.T) {
	service := New(nil, nil)
	torrent := &protocol.TorrentTaskContext{
		BindingID: "binding-1", BridgeInstanceID: "mp-main", QBClientID: "qb-main",
		TorrentHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)] = moviePilotTorrentRegistryEntry{Torrent: *torrent}

	service.syncMoviePilotTorrentStatus(t.Context(), torrent, "job-1", model.ClusterStageQBObserving, model.ClusterStageStatusRunning, 25, 100, "downloading", "", "", "", false)
	if got := service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]; got.Phase != model.MoviePilotTaskPhaseDownloading || got.DownloadProgress != .25 {
		t.Fatalf("downloading registry status = %#v", got)
	}
	service.syncMoviePilotTorrentStatus(t.Context(), torrent, "job-1", model.ClusterStageQBObserving, model.ClusterStageStatusSucceeded, 100, 100, "completed", "", "", "", true)
	if got := service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]; got.Phase != model.MoviePilotTaskPhaseDownloadComplete || got.DownloadProgress != 1 {
		t.Fatalf("download completed registry status = %#v", got)
	}
	service.syncMoviePilotTorrentStatus(t.Context(), torrent, "job-2", model.ClusterStageQBCopying, model.ClusterStageStatusRunning, 0, 0, "", "", "", "", false)
	if got := service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]; got.Phase != model.MoviePilotTaskPhaseStaging {
		t.Fatalf("staging registry status = %#v", got)
	}
	service.syncMoviePilotTorrentStatus(t.Context(), torrent, "job-2", model.ClusterStageUploadingMobile, model.ClusterStageStatusRunning, 50, 100, "uploading", "", "", "", false)
	if got := service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]; got.Phase != model.MoviePilotTaskPhaseUploading || got.UploadProgress != .5 {
		t.Fatalf("uploading registry status = %#v", got)
	}
	service.syncMoviePilotTorrentStatus(t.Context(), torrent, "job-2", model.ClusterStageUploadingMobile, model.ClusterStageStatusSucceeded, 100, 100, "uploaded", "", "", model.MoviePilotTaskPhaseCompleted, true)
	if got := service.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]; got.Phase != model.MoviePilotTaskPhaseCompleted || got.UploadProgress != 1 {
		t.Fatalf("completed registry status = %#v", got)
	}
}
