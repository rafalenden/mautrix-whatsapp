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
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// When someone adds members to a group, WhatsApp lets them share the group's
// recent messages with the new members. The sharer's device sends a
// MessageHistoryNotice (bridged as a notice) and a MessageHistoryBundle, an
// encrypted blob holding the shared messages. This downloads the blob and
// bridges its messages as if they had just arrived, so they land after the
// notice, in their original order and with their original timestamps.

const maxMessageHistoryBundleSize = 64 * 1024 * 1024

const messageHistoryBundleTimeout = 2 * time.Minute

// messageHistoryBundleMediaTypes are the media types the blob may be encrypted
// as, tried in order. WhatsApp doesn't document which one bundles use; history
// sync blobs use the history keys, so that's the first guess. A wrong guess
// fails the HMAC check, so trying the next one is safe.
var messageHistoryBundleMediaTypes = []struct {
	mediaType whatsmeow.MediaType
	mmsType   string
}{
	{whatsmeow.MediaHistory, "md-msg-hist"},
	{whatsmeow.MediaDocument, "document"},
}

func (wa *WhatsAppClient) handleWAMessageHistoryBundle(ctx context.Context, evt *events.Message, bundle *waE2E.MessageHistoryBundle) bool {
	log := zerolog.Ctx(ctx).With().
		Str("action", "handle message history bundle").
		Str("message_id", evt.Info.ID).
		Stringer("chat_jid", evt.Info.Chat).
		Stringer("sender_jid", evt.Info.Sender).
		Logger()
	ctx = log.WithContext(ctx)
	if !evt.Info.IsGroup {
		log.Debug().Msg("Ignoring message history bundle outside a group")
		return true
	}
	metadata := bundle.GetMessageHistoryMetadata()
	log.Debug().
		Str("mimetype", bundle.GetMimetype()).
		Int64("message_count", metadata.GetMessageCount()).
		Int64("oldest_message_ts", metadata.GetOldestMessageTimestampInBundle()).
		Msg("Received message history bundle")
	ctx, cancel := context.WithTimeout(ctx, messageHistoryBundleTimeout)
	defer cancel()
	data, err := wa.downloadMessageHistoryBundle(ctx, bundle)
	if err != nil {
		// Returning false would make the message be redelivered, which won't fix a
		// failed download, so the bundle is given up on.
		log.Err(err).Msg("Failed to download message history bundle")
		return true
	}
	messages, err := decodeMessageHistoryBundle(data)
	if err != nil {
		log.Err(err).
			Int("data_length", len(data)).
			Hex("data_prefix", data[:min(len(data), 64)]).
			Msg("Failed to decode message history bundle")
		log.Trace().Bytes("data", data).Msg("Content of message history bundle that failed to decode")
		return true
	}
	log.Info().Int("message_count", len(messages)).Msg("Bridging messages from message history bundle")
	bridged := 0
	for _, webMsg := range messages {
		msgEvt, err := wa.parseMessageHistoryBundleMessage(evt, webMsg)
		if err != nil {
			log.Warn().Err(err).
				Str("bundled_message_id", webMsg.GetKey().GetID()).
				Msg("Dropping message from message history bundle")
			continue
		}
		switch getMessageType(msgEvt.Message) {
		case "message history notice", "ignore":
			continue
		}
		if !wa.handleWAMessage(ctx, msgEvt) {
			continue
		}
		bridged++
		// The bundle is the sharer's, so reactions it marks as from me are theirs.
		target := pickLID(msgEvt.Info.Sender, msgEvt.Info.SenderAlt).ToNonAD()
		sharer := pickLID(evt.Info.Sender, evt.Info.SenderAlt)
		for _, reaction := range webMsg.GetReactions() {
			if reactionEvt := sharedHistoryReaction(evt.Info.Chat, sharer, false, target, msgEvt.Info.ID, reaction); reactionEvt != nil {
				wa.handleWAMessage(ctx, reactionEvt)
			}
		}
	}
	log.Info().Int("bridged_count", bridged).Msg("Finished bridging message history bundle")
	return true
}

func (wa *WhatsAppClient) downloadMessageHistoryBundle(ctx context.Context, bundle *waE2E.MessageHistoryBundle) ([]byte, error) {
	if bundle.GetDirectPath() == "" {
		return nil, whatsmeow.ErrNoURLPresent
	}
	var errs []error
	for _, candidate := range messageHistoryBundleMediaTypes {
		data, err := wa.Client.DownloadMediaWithPath(
			ctx, bundle.GetDirectPath(), bundle.GetFileEncSHA256(), bundle.GetFileSHA256(), bundle.GetMediaKey(),
			candidate.mediaType, candidate.mmsType, false,
		)
		if err == nil {
			zerolog.Ctx(ctx).Debug().
				Str("media_type", string(candidate.mediaType)).
				Int("data_length", len(data)).
				Msg("Downloaded message history bundle")
			return data, nil
		}
		errs = append(errs, fmt.Errorf("as %s: %w", candidate.mediaType, err))
		if !errors.Is(err, whatsmeow.ErrInvalidMediaHMAC) {
			// Only a wrong key is worth retrying with another media type.
			break
		}
	}
	return nil, errors.Join(errs...)
}

// decodeMessageHistoryBundle reads the messages out of a downloaded bundle,
// oldest first. The bundle's layout isn't documented, so this accepts a
// HistorySync or a single history sync Conversation, zlib-compressed or not,
// and keeps the first reading whose messages all look real.
func decodeMessageHistoryBundle(data []byte) ([]*waWeb.WebMessageInfo, error) {
	payloads := [][]byte{data}
	if inflated, err := inflate(data); err == nil {
		payloads = append([][]byte{inflated}, payloads...)
	}
	unmarshal := proto.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	for _, payload := range payloads {
		var historySync waHistorySync.HistorySync
		if unmarshal.Unmarshal(payload, &historySync) == nil {
			var messages []*waWeb.WebMessageInfo
			for _, conv := range historySync.GetConversations() {
				messages = append(messages, historySyncMessages(conv)...)
			}
			if plausibleMessages(messages) {
				return sortedOldestFirst(messages), nil
			}
		}
		var conv waHistorySync.Conversation
		if unmarshal.Unmarshal(payload, &conv) == nil {
			if messages := historySyncMessages(&conv); plausibleMessages(messages) {
				return sortedOldestFirst(messages), nil
			}
		}
	}
	return nil, errors.New("bundle isn't a history sync payload")
}

func inflate(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	inflated, err := io.ReadAll(io.LimitReader(reader, maxMessageHistoryBundleSize+1))
	if err != nil {
		return nil, err
	} else if len(inflated) > maxMessageHistoryBundleSize {
		return nil, errors.New("decompressed bundle is too large")
	}
	return inflated, nil
}

func historySyncMessages(conv *waHistorySync.Conversation) []*waWeb.WebMessageInfo {
	messages := make([]*waWeb.WebMessageInfo, 0, len(conv.GetMessages()))
	for _, msg := range conv.GetMessages() {
		if msg.GetMessage() != nil {
			messages = append(messages, msg.GetMessage())
		}
	}
	return messages
}

// plausibleMessages guards against a payload that happened to parse as the
// wrong message type, which protobuf allows: real messages have IDs and times.
func plausibleMessages(messages []*waWeb.WebMessageInfo) bool {
	if len(messages) == 0 {
		return false
	}
	for _, msg := range messages {
		if msg.GetKey().GetID() == "" || msg.GetMessageTimestamp() == 0 {
			return false
		}
	}
	return true
}

func sortedOldestFirst(messages []*waWeb.WebMessageInfo) []*waWeb.WebMessageInfo {
	slices.SortStableFunc(messages, func(a, b *waWeb.WebMessageInfo) int {
		return int(int64(a.GetMessageTimestamp()) - int64(b.GetMessageTimestamp()))
	})
	return messages
}

// parseMessageHistoryBundleMessage turns a bundled message into a message event
// in the bundle's group. The bundle comes from the sharer's device, so a message
// it marks as from me is the sharer's own: the user it was shared with is new to
// the group and had sent nothing there yet.
func (wa *WhatsAppClient) parseMessageHistoryBundleMessage(bundleEvt *events.Message, webMsg *waWeb.WebMessageInfo) (*events.Message, error) {
	if remoteJID := webMsg.GetKey().GetRemoteJID(); remoteJID != "" {
		if chat, err := types.ParseJID(remoteJID); err == nil && chat != bundleEvt.Info.Chat {
			return nil, fmt.Errorf("message is from another chat (%s)", chat)
		}
	}
	sharerMsg := webMsg.GetKey().GetFromMe()
	if sharerMsg {
		webMsg = proto.Clone(webMsg).(*waWeb.WebMessageInfo)
		webMsg.Key.FromMe = proto.Bool(false)
		webMsg.Key.Participant = proto.String(bundleEvt.Info.Sender.ToNonAD().String())
		webMsg.Participant = nil
	}
	evt, err := wa.Client.ParseWebMessage(bundleEvt.Info.Chat, webMsg)
	if err != nil {
		return nil, err
	}
	if sharerMsg {
		evt.Info.SenderAlt = bundleEvt.Info.SenderAlt
		evt.Info.AddressingMode = bundleEvt.Info.AddressingMode
		if evt.Info.PushName == "" {
			evt.Info.PushName = bundleEvt.Info.PushName
		}
	}
	return evt, nil
}
