package bootstrap

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/tache"
)

func TestDefaultConfigEnablesMoveTaskPersistence(t *testing.T) {
	config := conf.DefaultConfig(t.TempDir())
	if !config.Tasks.Move.TaskPersistant {
		t.Fatal("move task persistence is disabled by default")
	}
}

func TestMoveTaskManagerPersistsOnlyOrdinaryTasksWhenLegacyFlagEnabled(t *testing.T) {
	conf.Conf = conf.DefaultConfig(t.TempDir())
	database := openSQLiteTestDB(t, filepath.Join(t.TempDir(), "move-persistence.db"))
	db.Init(database)
	if err := database.Create(&model.TaskItem{Key: "move", PersistData: "[]"}).Error; err != nil {
		t.Fatalf("seed move task item: %v", err)
	}
	conf.Conf.Tasks.Move.TaskPersistant = true
	conf.SendStoragesLoadedSignal()
	manager := newMoveTaskManager(tache.WithRunning(false), tache.WithPersistDebounce(0*time.Second))
	manager.Add(&fs.FileTransferTask{ClusterBinding: &task_group.ClusterTransferBinding{}})
	manager.Add(&fs.FileTransferTask{TaskData: fs.TaskData{SrcActualPath: "ordinary-source"}})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var tasks []fs.FileTransferTask
		persisted, err := db.GetTaskDataByType("move")
		if err != nil {
			t.Fatalf("read persisted move tasks: %v", err)
		}
		if err := json.Unmarshal([]byte(persisted.PersistData), &tasks); err != nil {
			t.Fatalf("decode persisted move tasks: %v", err)
		}
		if len(tasks) == 1 && tasks[0].ClusterBinding == nil && tasks[0].SrcActualPath == "ordinary-source" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ordinary-only move persistence was not written before timeout")
}
