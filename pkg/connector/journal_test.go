package connector

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestJournalledEvents(t *testing.T) {
	for _, evt := range []any{&events.Receipt{}, &events.Star{}, &events.Presence{}, &events.CallTerminate{}, &events.UndecryptableMessage{}} {
		if !journalled(evt) {
			t.Errorf("%T isn't bridged, so it should be journalled", evt)
		}
	}
	for _, evt := range []any{&events.Message{}, &events.HistorySync{}, &events.ChatPresence{}, &events.Connected{}} {
		if journalled(evt) {
			t.Errorf("%T shouldn't be journalled", evt)
		}
	}
}

func TestJournalAppendsALineAnEvent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal")
	var j journal
	at := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	receipt := &events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("123", types.GroupServer), Sender: types.NewJID("456", types.HiddenUserServer)},
		MessageIDs:    []types.MessageID{"ABC"},
		Timestamp:     at.Add(-time.Hour),
		Type:          types.ReceiptTypePlayed,
	}
	receipts := []*waWeb.UserReceipt{{UserJID: proto.String("456@lid"), ReceiptTimestamp: proto.Int64(100), ReadTimestamp: proto.Int64(200)}}
	for _, evt := range []any{receipt, map[string]any{"receipts": receipts}} {
		line, err := json.Marshal(journalEntry{Time: at, Login: "1", Type: "Test", Event: journalValue(evt)})
		if err != nil {
			t.Fatal(err)
		}
		if err = j.append(dir, at, line); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.Open(filepath.Join(dir, "2026-11.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry map[string]any
		if err = json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, entry)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	first := lines[0]["event"].(map[string]any)
	if first["Type"] != "played" || first["Timestamp"] != "2026-11-26T11:00:00Z" || first["Chat"] != "123@g.us" {
		t.Errorf("the receipt lost its kind, time or chat: %v", first)
	}
	kept := lines[1]["event"].(map[string]any)["receipts"].([]any)[0].(map[string]any)
	if kept["userJID"] != "456@lid" || kept["readTimestamp"] != "200" {
		t.Errorf("history's receipt lost who or when: %v", kept)
	}
}
