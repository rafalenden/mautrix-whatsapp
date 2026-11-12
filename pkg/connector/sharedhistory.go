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
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/commands"

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
		Description: "Ask your phone for the messages from before the oldest one in this chat, such as the history shared when you were added to a group, and bridge them.",
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
	if err = login.Client.(*WhatsAppClient).requestSharedHistory(ce.Ctx, portalJID); err != nil {
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
	if err := wa.requestSharedHistory(ctx, portalJID); err != nil {
		zerolog.Ctx(ctx).Err(err).Stringer("portal_jid", portalJID).Msg("Failed to request shared history from phone")
	}
}

func (wa *WhatsAppClient) requestSharedHistory(ctx context.Context, portalJID types.JID) error {
	if wa.Client == nil || !wa.Client.IsLoggedIn() {
		return errors.New("not logged in")
	}
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, wa.makeWAPortalKey(portalJID))
	if err != nil {
		return fmt.Errorf("failed to get portal: %w", err)
	} else if portal == nil {
		return errors.New("chat isn't bridged")
	}
	anchor, err := wa.Main.Bridge.DB.Message.GetFirstPortalMessageExcludingIDPattern(ctx, portal.PortalKey, "fake:%")
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
	bridged := 0
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
		if wa.handleWAMessage(ctx, evt) {
			bridged++
		}
	}
	log.Info().Int("bridged_count", bridged).Msg("Finished bridging shared history")
	return true
}
