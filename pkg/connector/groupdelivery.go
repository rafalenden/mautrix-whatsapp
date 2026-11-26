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
	"context"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// groupDeliveryWindow is how long deliveries of one message are gathered before
// those of the same second are reported together. Everyone online gets a message within a moment, and
// a big group would otherwise mean an event per member for each message.
const groupDeliveryWindow = time.Second

// groupDeliveries holds, for each message of ours, the group members it has just
// been delivered to and that haven't been reported to Matrix yet.
type groupDeliveries struct {
	lock    sync.Mutex
	pending map[groupDeliveryKey]*pendingGroupDelivery
}

// A message, and the second WhatsApp says it reached someone. Deliveries are
// reported with that time, so only those that share it go out together.
type groupDeliveryKey struct {
	event  id.EventID
	second int64
}

type pendingGroupDelivery struct {
	info  bridgev2.MessageStatusEventInfo
	users []id.UserID
	// When WhatsApp says they got it.
	at time.Time
}

// handleGroupDelivery tells Matrix that a group's member has received messages
// of ours, the way bridgev2 does for the other person in a one-to-one chat: a
// message status naming them in delivered_to_users. bridgev2 itself drops
// delivery receipts outside one-to-one chats.
func (wa *WhatsAppClient) handleGroupDelivery(ctx context.Context, chat, member types.JID, at time.Time, targets []networkid.MessageID) {
	if wa.IsOwnJID(member) {
		// One of our own devices got it.
		return
	}
	log := zerolog.Ctx(ctx)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, wa.makeWAPortalKey(chat))
	if err != nil {
		log.Err(err).Stringer("chat_id", chat).Msg("Failed to get portal for group delivery receipt")
		return
	} else if portal == nil || portal.MXID == "" {
		return
	}
	matrix := wa.Main.Bridge.Matrix
	user := matrix.FormatGhostMXID(waid.MakeUserID(member))
	for _, target := range targets {
		parts, err := wa.Main.Bridge.DB.Message.GetAllPartsByID(ctx, portal.Receiver, target)
		if err != nil {
			log.Err(err).Str("target_id", string(target)).Msg("Failed to get target message for group delivery receipt")
			return
		} else if len(parts) == 0 {
			continue
		} else if _, sentByGhost := matrix.ParseGhostMXID(parts[0].SenderMXID); sentByGhost {
			continue
		}
		for _, part := range parts {
			wa.queueGroupDelivery(bridgev2.MessageStatusEventInfo{
				RoomID:        portal.MXID,
				SourceEventID: part.MXID,
				Sender:        part.SenderMXID,

				IsSourceEventDoublePuppeted: part.IsDoublePuppeted,
			}, user, at)
		}
	}
}

func (wa *WhatsAppClient) queueGroupDelivery(info bridgev2.MessageStatusEventInfo, user id.UserID, at time.Time) {
	deliveries := &wa.groupDeliveries
	deliveries.lock.Lock()
	defer deliveries.lock.Unlock()
	key := groupDeliveryKey{event: info.SourceEventID, second: at.Unix()}
	if waiting, ok := deliveries.pending[key]; ok {
		if !slices.Contains(waiting.users, user) {
			waiting.users = append(waiting.users, user)
		}
		return
	}
	if deliveries.pending == nil {
		deliveries.pending = make(map[groupDeliveryKey]*pendingGroupDelivery)
	}
	deliveries.pending[key] = &pendingGroupDelivery{info: info, users: []id.UserID{user}, at: at}
	time.AfterFunc(groupDeliveryWindow, func() {
		wa.sendGroupDeliveries(key)
	})
}

func (wa *WhatsAppClient) sendGroupDeliveries(key groupDeliveryKey) {
	deliveries := &wa.groupDeliveries
	deliveries.lock.Lock()
	waiting := deliveries.pending[key]
	delete(deliveries.pending, key)
	deliveries.lock.Unlock()
	if waiting == nil {
		return
	}
	ctx := wa.UserLogin.Log.With().Str("action", "group delivery receipt").Logger().WithContext(wa.Main.Bridge.BackgroundCtx)
	status := &bridgev2.MessageStatus{
		Status:      event.MessageStatusSuccess,
		DeliveredTo: waiting.users,
	}
	// SendMessageStatus dates its event now. One that waited on the server while
	// the bridge was off is sent with WhatsApp's own time instead, where the
	// bridge can send it itself.
	connector, ok := wa.Main.Bridge.Matrix.(*matrix.Connector)
	if !ok || !connector.Config.Matrix.MessageStatusEvents || waiting.at.IsZero() || time.Since(waiting.at) < time.Minute {
		wa.Main.Bridge.Matrix.SendMessageStatus(ctx, status, &waiting.info)
		return
	}
	_, err := wa.Main.Bridge.Bot.SendMessage(ctx, waiting.info.RoomID, event.BeeperMessageStatus, &event.Content{
		Parsed: status.ToMSSEvent(&waiting.info),
	}, &bridgev2.MatrixSendExtra{Timestamp: waiting.at})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Stringer("event_id", key.event).Msg("Failed to send group delivery status")
	}
}
