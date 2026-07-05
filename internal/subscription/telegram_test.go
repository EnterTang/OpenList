package subscription

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestParseTelegramRowsEnvelopeAndLinks(t *testing.T) {
	body := []byte(`{
		"results": [{
			"msgId": 123,
			"channel": "@movies",
			"text": "新剧 https://pan.example/s/abc 提取码 abcd",
			"links": ["https://pan.example/s/abc"],
			"buttons": [{"text": "open", "url": "https://pan.example/s/def"}]
		}]
	}`)
	rows, err := parseTelegramRows(body)
	if err != nil {
		t.Fatalf("parse rows: %v", err)
	}
	if len(rows) != 1 || rowMessageID(rows[0]) != 123 {
		t.Fatalf("rows = %#v", rows)
	}
	links := rowLinks(rows[0])
	if len(links) != 2 {
		t.Fatalf("links = %#v, want 2", links)
	}
	if got := normalizeTelegramLinkWithAccessCode(links[0], rowAccessCode(rows[0])); got != "https://pan.example/s/abc,abcd" {
		t.Fatalf("normalized link = %q", got)
	}
}

func TestTelegramLinkItemUsesStableMessageSourceKey(t *testing.T) {
	row := telegramCommandRow{MsgID: float64(456), Channel: "@movies"}
	item := telegramLinkItem(&model.Subscription{ID: 7}, row, "https://pan.example/s/abc", time.Now())
	if item.SourceKey == "" || item.SourceURL != "https://pan.example/s/abc" {
		t.Fatalf("item = %#v", item)
	}
	if item.Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("status = %q", item.Status)
	}

	body, err := json.Marshal(item)
	if err != nil || len(body) == 0 {
		t.Fatalf("marshal item: body=%s err=%v", body, err)
	}
}
