package db

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func BenchmarkListLatestSubscriptionTelegramEventsMillionRows(b *testing.B) {
	if os.Getenv("OPENLIST_LARGE_DB_BENCH") != "1" {
		b.Skip("set OPENLIST_LARGE_DB_BENCH=1 to seed the million-row fixture")
	}

	previousConf := conf.Conf
	conf.Conf = conf.DefaultConfig(b.TempDir())
	database, err := gorm.Open(sqlite.Open(filepath.Join(b.TempDir(), "subscription-benchmark.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: conf.Conf.Database.TablePrefix},
		Logger:         logger.Discard,
	})
	if err != nil {
		b.Fatalf("open benchmark database: %v", err)
	}
	Init(database)
	b.Cleanup(func() {
		conf.Conf = previousConf
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	const subscriptionCount = 151
	subscriptions := make([]model.Subscription, subscriptionCount)
	for i := range subscriptions {
		subscriptions[i] = model.Subscription{
			Name:     "Million row benchmark " + strconv.Itoa(i),
			TMDBName: "Million row benchmark " + strconv.Itoa(i),
		}
	}
	if err := database.CreateInBatches(&subscriptions, 100).Error; err != nil {
		b.Fatalf("create subscriptions: %v", err)
	}
	const subscriptionItemCount = 3708
	items := make([]model.SubscriptionItem, subscriptionItemCount)
	for i := range items {
		items[i] = model.SubscriptionItem{
			SubscriptionID: subscriptions[i%subscriptionCount].ID,
			SourceKey:      "source-" + strconv.Itoa(i),
			FileName:       "file-" + strconv.Itoa(i),
		}
	}
	if err := database.CreateInBatches(&items, 100).Error; err != nil {
		b.Fatalf("create subscription items: %v", err)
	}
	const eventCount = 1_000_000
	createdAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).Add(-eventCount * time.Second)
	events := make([]model.SubscriptionTelegramEvent, eventCount)
	for i := range events {
		subscriptionIndex := i % subscriptionCount
		events[i] = model.SubscriptionTelegramEvent{
			SubscriptionID: subscriptions[subscriptionIndex].ID,
			Channel:        "benchmark",
			MessageID:      "message-" + strconv.Itoa(i),
			CreatedAt:      createdAt.Add(time.Duration(i) * time.Second),
			Status:         model.SubscriptionTelegramEventStatusProcessed,
		}
	}
	if err := database.CreateInBatches(&events, 1000).Error; err != nil {
		b.Fatalf("seed event history: %v", err)
	}
	subscriptionIDs := make([]uint, len(subscriptions))
	for i := range subscriptions {
		subscriptionIDs[i] = subscriptions[i].ID
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		latest, err := ListLatestSubscriptionTelegramEventsBySubscriptionIDs(subscriptionIDs)
		if err != nil {
			b.Fatal(err)
		}
		if len(latest) != subscriptionCount {
			b.Fatalf("latest events = %#v", latest)
		}
	}
}
