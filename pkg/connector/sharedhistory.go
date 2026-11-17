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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// History shared with us when we were added to a group predates everything the
// bridge has in that chat, so the phone, which keeps it, is asked for the
// messages before the oldest one bridged. They're bridged like new messages, with
// their own timestamps, and any the chat already has are skipped. This catches
// up on shares whose bundle the bridge missed or couldn't read.

// sharedHistoryRequestCount is how many messages to ask for. WhatsApp shares at
// most 100.
const sharedHistoryRequestCount = 100

// sharedHistoryRequestDelay gives the phone time to take in a bundle it received
// along with ours before it's asked for the messages.
const sharedHistoryRequestDelay = 30 * time.Second

// sharedHistoryRequestCooldown stops a chat from being asked again while a
// request is out or was just answered.
const sharedHistoryRequestCooldown = 10 * time.Minute

var cmdFetchSharedHistory = &commands.FullHandler{
	Func: fnFetchSharedHistory,
	Name: "fetch-shared-history",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionChats,
		Description: "Ask your phone for the messages from before the oldest one in this chat (or before the given message), such as the history shared when you were added to a group, and bridge them.",
		Args:        "[_message ID_]",
	},
	RequiresLogin:  true,
	RequiresPortal: true,
}

func fnFetchSharedHistory(ce *commands.Event) {
	login, _, err := ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false)
	if err != nil {
		ce.Reply("Failed to find your login in this chat: %v", err)
		return
	} else if login == nil {
		ce.Reply("You're not in this chat on WhatsApp")
		return
	}
	portalJID, err := waid.ParsePortalID(ce.Portal.ID)
	if err != nil {
		ce.Reply("Failed to parse chat ID: %v", err)
		return
	}
	// An argument names the message to fetch history before, by its full ID,
	// instead of the chat's oldest one.
	var before networkid.MessageID
	if len(ce.Args) > 0 {
		before = networkid.MessageID(ce.Args[0])
	}
	if err = login.Client.(*WhatsAppClient).requestSharedHistory(ce.Ctx, portalJID, before); err != nil {
		ce.Reply("Failed to ask your phone for the history: %v", err)
		return
	}
	ce.React("✅")
}

// isSharedWithMe says whether a history share notice lists us among the people
// the history went to.
func (wa *WhatsAppClient) isSharedWithMe(notice *waE2E.MessageHistoryNotice) bool {
	for _, receiver := range notice.GetMessageHistoryMetadata().GetHistoryReceivers() {
		user, _, _ := strings.Cut(receiver, "@")
		user, _, _ = strings.Cut(user, ":")
		if user != "" && (user == wa.JID.User || user == wa.LID.User) {
			return true
		}
	}
	return false
}

func (wa *WhatsAppClient) requestSharedHistoryLater(ctx context.Context, portalJID types.JID) {
	select {
	case <-time.After(sharedHistoryRequestDelay):
	case <-ctx.Done():
		return
	}
	if err := wa.requestSharedHistory(ctx, portalJID, ""); err != nil {
		zerolog.Ctx(ctx).Err(err).Stringer("portal_jid", portalJID).Msg("Failed to request shared history from phone")
	}
}

// requestSharedHistory asks the phone for the messages before one in the chat:
// the given one, or the oldest one bridged.
func (wa *WhatsAppClient) requestSharedHistory(ctx context.Context, portalJID types.JID, before networkid.MessageID) error {
	if wa.Client == nil || !wa.Client.IsLoggedIn() {
		return errors.New("not logged in")
	}
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, wa.makeWAPortalKey(portalJID))
	if err != nil {
		return fmt.Errorf("failed to get portal: %w", err)
	} else if portal == nil {
		return errors.New("chat isn't bridged")
	}
	var anchor *database.Message
	if before != "" {
		anchor, err = wa.Main.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, before)
	} else {
		anchor, err = wa.Main.Bridge.DB.Message.GetFirstPortalMessageExcludingIDPattern(ctx, portal.PortalKey, "fake:%")
	}
	if err != nil {
		return fmt.Errorf("failed to get oldest message: %w", err)
	} else if anchor == nil {
		return errors.New("chat has no messages to ask for history before")
	}
	parsed, err := waid.ParseMessageID(anchor.ID)
	if err != nil {
		return fmt.Errorf("failed to parse oldest message's ID: %w", err)
	}

	msgID := wa.Client.GenerateMessageID()
	wa.onDemandHistorySyncLock.Lock()
	if last, ok := wa.sharedHistoryRequested[portalJID]; ok && time.Since(last) < sharedHistoryRequestCooldown {
		wa.onDemandHistorySyncLock.Unlock()
		zerolog.Ctx(ctx).Debug().Stringer("portal_jid", portalJID).Msg("Shared history was requested recently, not asking again")
		return nil
	}
	wa.sharedHistoryRequested[portalJID] = time.Now()
	wa.sharedHistoryRequests[msgID] = portalJID
	wa.onDemandHistorySyncLock.Unlock()

	reqData := wa.Client.BuildHistorySyncRequest(&types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     portalJID,
			Sender:   parsed.Sender,
			IsFromMe: wa.IsOwnJID(parsed.Sender),
			IsGroup:  portalJID.Server == types.GroupServer,
		},
		ID:        parsed.ID,
		Timestamp: anchor.Timestamp,
	}, sharedHistoryRequestCount)
	zerolog.Ctx(ctx).Info().
		Str("request_msg_id", msgID).
		Stringer("portal_jid", portalJID).
		Str("anchor_msg_id", parsed.ID).
		Time("anchor_ts", anchor.Timestamp).
		Msg("Asking phone for shared history")
	_, err = wa.Client.SendMessage(ctx, wa.JID.ToNonAD(), reqData, whatsmeow.SendRequestExtra{ID: msgID, Peer: true})
	if err != nil {
		wa.onDemandHistorySyncLock.Lock()
		delete(wa.sharedHistoryRequests, msgID)
		delete(wa.sharedHistoryRequested, portalJID)
		wa.onDemandHistorySyncLock.Unlock()
		return fmt.Errorf("failed to send request: %w", err)
	}
	return nil
}

// handleSharedHistorySync bridges the phone's answer to a shared history request.
// It reports false for on-demand history syncs that answer something else.
func (wa *WhatsAppClient) handleSharedHistorySync(ctx context.Context, sessionID string, blob *waHistorySync.HistorySync) bool {
	wa.onDemandHistorySyncLock.Lock()
	portalJID, ok := wa.sharedHistoryRequests[sessionID]
	delete(wa.sharedHistoryRequests, sessionID)
	wa.onDemandHistorySyncLock.Unlock()
	if !ok {
		return false
	}
	log := zerolog.Ctx(ctx).With().
		Str("action", "handle shared history").
		Str("request_msg_id", sessionID).
		Stringer("portal_jid", portalJID).
		Logger()
	ctx = log.WithContext(ctx)
	var messages []*waWeb.WebMessageInfo
	for _, conv := range blob.GetConversations() {
		for _, msg := range historySyncMessages(conv) {
			// Stubs (joins, setting changes) have no message to bridge live.
			if msg.GetMessage() != nil && msg.GetKey().GetID() != "" {
				messages = append(messages, msg)
			}
		}
	}
	messages = sortedOldestFirst(messages)
	log.Info().
		Int("conversation_count", len(blob.GetConversations())).
		Int("message_count", len(messages)).
		Msg("Phone answered shared history request")
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, wa.makeWAPortalKey(portalJID))
	if err != nil || portal == nil || portal.MXID == "" {
		log.Warn().Err(err).Msg("Chat for shared history isn't bridged")
		return true
	}
	wasRead := wa.readToEnd(ctx, portal.MXID)
	bridged, added := 0, 0
	var lastID networkid.MessageID
	for _, webMsg := range messages {
		evt, err := wa.Client.ParseWebMessage(portalJID, webMsg)
		if err != nil {
			log.Warn().Err(err).Str("msg_id", webMsg.GetKey().GetID()).Msg("Dropping message from shared history")
			continue
		}
		switch getMessageType(evt.Message) {
		case "message history notice", "ignore":
			continue
		}
		// The message's ID uses the sender's other JID, which this fills in.
		if !wa.ensureAltJIDs(ctx, &evt.Info.MessageSource, true) {
			continue
		}
		msgID := waid.MakeMessageIDWithAltSender(evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt, evt.Info.ID)
		existing, err := wa.Main.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, msgID)
		if err != nil {
			log.Err(err).Str("msg_id", evt.Info.ID).Msg("Failed to check whether shared message is already bridged")
		}
		target := pickLID(evt.Info.Sender, evt.Info.SenderAlt).ToNonAD()
		if !wa.handleWAMessage(ctx, evt) {
			continue
		}
		bridged++
		if existing == nil {
			added++
			lastID = msgID
		}
		for _, reaction := range webMsg.GetReactions() {
			if reactionEvt := sharedHistoryReaction(portalJID, wa.GetLID(), true, target, evt.Info.ID, reaction); reactionEvt != nil {
				wa.handleWAMessage(ctx, reactionEvt)
			}
		}
	}
	log.Info().
		Int("bridged_count", bridged).
		Int("added_count", added).
		Bool("chat_was_read", wasRead).
		Msg("Finished bridging shared history")
	// Only history that's new to the chat sits after the read marker.
	if wasRead && lastID != "" {
		wa.markReadOnceBridged(ctx, portal, lastID)
	}
	return true
}

// sharedHistoryReaction turns a reaction listed on a history message into the
// event the reaction would have arrived as. A history message's reactions come
// with it rather than as messages of their own.
// own is whose history it is: reactions it marks as from me are theirs.
func sharedHistoryReaction(chat, own types.JID, ownIsMe bool, target types.JID, targetID types.MessageID, reaction *waWeb.Reaction) *events.Message {
	if reaction.GetText() == "" || reaction.GetKey().GetID() == "" {
		return nil
	}
	var sender types.JID
	isMe := false
	if reaction.GetKey().GetFromMe() {
		sender, isMe = own.ToNonAD(), ownIsMe
	} else {
		sender, _ = types.ParseJID(reaction.GetKey().GetParticipant())
	}
	if sender.IsEmpty() {
		return nil
	}
	return sharedReactionEvent(chat, sender, isMe, target, targetID, reaction)
}

func sharedReactionEvent(chat, sender types.JID, isMe bool, target types.JID, targetID types.MessageID, reaction *waWeb.Reaction) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   sender,
				IsFromMe: isMe,
				IsGroup:  chat.Server == types.GroupServer,
			},
			ID:        reaction.GetKey().GetID(),
			Timestamp: time.UnixMilli(reaction.GetSenderTimestampMS()),
		},
		Message: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
			// The key is as the reactor's phone would write it: by the target's sender.
			Key: &waCommon.MessageKey{
				RemoteJID:   proto.String(chat.String()),
				FromMe:      proto.Bool(false),
				ID:          proto.String(targetID),
				Participant: proto.String(target.String()),
			},
			Text:              proto.String(reaction.GetText()),
			SenderTimestampMS: proto.Int64(reaction.GetSenderTimestampMS()),
		}},
	}
}

// doublePuppet is the user's own Matrix account, as the bridge acts for it.
func (wa *WhatsAppClient) doublePuppet(ctx context.Context) *appservice.IntentAPI {
	if intent, ok := wa.UserLogin.User.DoublePuppet(ctx).(*matrix.ASIntent); ok {
		return intent.Matrix()
	}
	return nil
}

// readToEnd says whether the user has read a chat up to its last message.
// History added to the chat arrives after the read marker, so it would show up
// as unread, which it shouldn't if everything before it had been read.
// Reactions and the like after the marker don't count, as they don't notify.
func (wa *WhatsAppClient) readToEnd(ctx context.Context, roomID id.RoomID) bool {
	intent := wa.doublePuppet(ctx)
	if intent == nil {
		return false
	}
	var marker event.FullyReadEventContent
	if err := intent.GetRoomAccountData(ctx, roomID, event.AccountDataFullyRead.Type, &marker); err != nil || marker.EventID == "" {
		return false
	}
	resp, err := intent.Messages(ctx, roomID, "", "", mautrix.DirectionBackward, nil, 50)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to read chat's latest events")
		return false
	}
	for _, evt := range resp.Chunk {
		if evt.ID == marker.EventID {
			return true
		} else if (evt.Type == event.EventMessage || evt.Type == event.EventEncrypted) && evt.Sender != intent.UserID {
			return false
		}
	}
	return false
}

// markReadOnceBridged moves the read marker to a message once it's in the chat.
// Messages are bridged in the portal's own time, so this waits for it.
func (wa *WhatsAppClient) markReadOnceBridged(ctx context.Context, portal *bridgev2.Portal, msgID networkid.MessageID) {
	intent := wa.UserLogin.User.DoublePuppet(ctx)
	if intent == nil {
		return
	}
	for range 60 {
		msg, err := wa.Main.Bridge.DB.Message.GetLastPartByID(ctx, portal.Receiver, msgID)
		if err == nil && msg != nil && msg.MXID != "" {
			if err = intent.MarkRead(ctx, portal.MXID, msg.MXID, time.Now()); err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("Failed to mark shared history read")
			}
			return
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}
	zerolog.Ctx(ctx).Warn().Str("msg_id", string(msgID)).Msg("Shared history didn't reach the chat in time to mark it read")
}
