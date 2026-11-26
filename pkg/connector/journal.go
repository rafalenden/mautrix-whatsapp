// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// journal keeps what WhatsApp sends only once and the bridge doesn't put on
// Matrix: receipts with their own times, events with no handler, history it
// couldn't parse. One JSON object a line, in a file a month, in the folder the
// journal_dir option names; nothing is kept while that's empty. Nothing reads
// it back yet, and nothing is ever removed from it.
type journal struct {
	lock  sync.Mutex
	month string
	file  *os.File
}

type journalEntry struct {
	// When the bridge got it. An event's own time, where it has one, is in it.
	Time  time.Time `json:"time"`
	Login string    `json:"login"`
	Type  string    `json:"type"`
	Event any       `json:"event"`
}

// journalled says whether an event goes into the journal: all of them, new
// kinds included, but for those listed here.
func journalled(rawEvt any) bool {
	switch rawEvt.(type) {
	case *events.Message, *events.FBMessage:
		// Bridged whole.
		return false
	case *events.HistorySync:
		// Kept in the bridge's database until it's backfilled. What isn't
		// bridged of it is journalled as it's stored (backfill.go).
		return false
	case *events.ChatPresence:
		// Someone typing, a moment ago.
		return false
	case *events.Connected, *events.Disconnected, *events.KeepAliveTimeout, *events.KeepAliveRestored,
		*events.OfflineSyncPreview, *events.OfflineSyncCompleted, *events.AppStateSyncComplete,
		*events.QR, *events.PairSuccess, *events.PairError, *events.StreamReplaced, *events.ManualLoginReconnect:
		// The connection's own, with nothing from anyone in them.
		return false
	default:
		return true
	}
}

func (wa *WhatsAppClient) journalEvent(rawEvt any) {
	if wa.Main.Config.JournalDir == "" || !journalled(rawEvt) {
		return
	}
	wa.writeJournal(strings.TrimPrefix(fmt.Sprintf("%T", rawEvt), "*events."), rawEvt)
}

// journalHistoryMessage keeps what's lost of a message in synced history: who
// had received and read it and when, and all of it when it's being dropped.
func (wa *WhatsAppClient) journalHistoryMessage(chat types.JID, msg *waWeb.WebMessageInfo, dropped bool) {
	if wa.Main.Config.JournalDir == "" || msg == nil {
		return
	}
	if dropped {
		wa.writeJournal("DroppedHistoryMessage", map[string]any{"chat": chat, "message": msg})
	} else if len(msg.GetUserReceipt()) > 0 {
		wa.writeJournal("HistoryReceipts", map[string]any{
			"chat":      chat,
			"key":       msg.GetKey(),
			"timestamp": msg.GetMessageTimestamp(),
			"receipts":  msg.GetUserReceipt(),
		})
	}
}

func (wa *WhatsAppClient) writeJournal(kind string, evt any) {
	now := time.Now()
	line, err := json.Marshal(journalEntry{Time: now, Login: string(wa.UserLogin.ID), Type: kind, Event: journalValue(evt)})
	if err != nil {
		// Something in it has no JSON form. Its printed form still has the data.
		line, err = json.Marshal(journalEntry{Time: now, Login: string(wa.UserLogin.ID), Type: kind, Event: fmt.Sprintf("%+v", evt)})
	}
	if err == nil {
		err = wa.Main.journal.append(wa.Main.Config.JournalDir, now, line)
	}
	if err != nil {
		wa.UserLogin.Log.Err(err).Str("journal_type", kind).Msg("Failed to write to journal")
	}
}

// journalValue gives protobuf messages their own JSON form, with every field.
func journalValue(evt any) any {
	switch typed := evt.(type) {
	case proto.Message:
		if data, err := protojson.Marshal(typed); err == nil {
			return json.RawMessage(data)
		}
	case map[string]any:
		converted := make(map[string]any, len(typed))
		for key, value := range typed {
			converted[key] = journalValue(value)
		}
		return converted
	case []*waWeb.UserReceipt:
		converted := make([]any, len(typed))
		for i, value := range typed {
			converted[i] = journalValue(value)
		}
		return converted
	}
	return evt
}

func (j *journal) append(dir string, now time.Time, line []byte) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	month := now.UTC().Format("2006-01")
	if j.file == nil || j.month != month {
		if j.file != nil {
			_ = j.file.Close()
			j.file = nil
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(dir, month+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		j.file, j.month = file, month
	}
	_, err := j.file.Write(append(line, '\n'))
	return err
}
