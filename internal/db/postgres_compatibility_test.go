package db

import (
	"os"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestPostgresSubscriptionCompatibility(t *testing.T) {
	dsn := os.Getenv("OPENLIST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set OPENLIST_POSTGRES_DSN to run PostgreSQL compatibility tests")
	}

	previousConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Database.Type = "postgres"
	conf.Conf.Database.DSN = dsn
	conf.Conf.Database.TablePrefix = "compat_"
	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: conf.Conf.Database.TablePrefix},
	})
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	Init(database)
	t.Cleanup(func() {
		conf.Conf = previousConf
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	subscription := model.Subscription{Name: "PostgreSQL compatibility", TMDBName: "PostgreSQL compatibility"}
	if err := database.Create(&subscription).Error; err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	createdAt := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	events := []model.SubscriptionTelegramEvent{
		{SubscriptionID: subscription.ID, Channel: "source", MessageID: "old", CreatedAt: createdAt.Add(-time.Minute)},
		{SubscriptionID: subscription.ID, Channel: "source", MessageID: "latest", CreatedAt: createdAt},
	}
	if err := database.Create(&events).Error; err != nil {
		t.Fatalf("create events: %v", err)
	}
	latest, err := ListLatestSubscriptionTelegramEventsBySubscriptionIDs([]uint{subscription.ID})
	if err != nil {
		t.Fatalf("list latest postgres event: %v", err)
	}
	if len(latest) != 1 || latest[0].MessageID != "latest" {
		t.Fatalf("latest events = %#v", latest)
	}

	inbox := model.ClusterInbox{
		ID: "compat-inbox", MessageID: "compat-message", PeerNodeID: "node", SessionID: "session", Seq: 1,
		Status: model.ClusterMessageStatusProcessed,
	}
	if err := database.Create(&inbox).Error; err != nil {
		t.Fatalf("create cluster inbox: %v", err)
	}
	duplicate := model.ClusterInbox{
		ID: "compat-duplicate", MessageID: inbox.MessageID, PeerNodeID: "node", SessionID: "session", Seq: 2,
	}
	if err := database.Create(&duplicate).Error; err == nil {
		t.Fatal("duplicate cluster inbox message was accepted")
	}
}
