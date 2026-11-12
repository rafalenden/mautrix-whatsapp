package connector

import (
	"bytes"
	"compress/zlib"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const testGroup = "120363000000000000@g.us"

func testWebMessage(id string, ts uint64, fromMe bool, participant, text string) *waWeb.WebMessageInfo {
	key := &waCommon.MessageKey{
		RemoteJID: proto.String(testGroup),
		FromMe:    proto.Bool(fromMe),
		ID:        proto.String(id),
	}
	if participant != "" {
		key.Participant = proto.String(participant)
	}
	return &waWeb.WebMessageInfo{
		Key:              key,
		MessageTimestamp: proto.Uint64(ts),
		Message:          &waE2E.Message{Conversation: proto.String(text)},
	}
}

func testConversation() *waHistorySync.Conversation {
	return &waHistorySync.Conversation{
		ID: proto.String(testGroup),
		Messages: []*waHistorySync.HistorySyncMsg{
			{Message: testWebMessage("C", 300, false, "111@lid", "third")},
			{Message: testWebMessage("A", 100, false, "222@lid", "first")},
			{Message: testWebMessage("B", 200, true, "", "second")},
		},
	}
}

func deflate(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func assertOrder(t *testing.T, messages []*waWeb.WebMessageInfo, ids ...string) {
	t.Helper()
	if len(messages) != len(ids) {
		t.Fatalf("got %d messages, want %d", len(messages), len(ids))
	}
	for i, id := range ids {
		if got := messages[i].GetKey().GetID(); got != id {
			t.Errorf("message %d is %s, want %s", i, got, id)
		}
	}
}

func TestDecodeMessageHistoryBundle(t *testing.T) {
	historySync, err := proto.Marshal(&waHistorySync.HistorySync{
		SyncType:      waHistorySync.HistorySync_ON_DEMAND.Enum(),
		Conversations: []*waHistorySync.Conversation{testConversation()},
	})
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := proto.Marshal(testConversation())
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"compressed history sync": deflate(t, historySync),
		"history sync":            historySync,
		"compressed conversation": deflate(t, conversation),
		"conversation":            conversation,
	} {
		t.Run(name, func(t *testing.T) {
			messages, err := decodeMessageHistoryBundle(data)
			if err != nil {
				t.Fatal(err)
			}
			assertOrder(t, messages, "A", "B", "C")
		})
	}
}

func TestDecodeMessageHistoryBundleRejectsJunk(t *testing.T) {
	for name, data := range map[string][]byte{
		"empty":      {},
		"text":       []byte("not a protobuf at all"),
		"compressed": deflate(t, []byte("still not a protobuf")),
	} {
		t.Run(name, func(t *testing.T) {
			if messages, err := decodeMessageHistoryBundle(data); err == nil {
				t.Fatalf("decoded %d messages from junk", len(messages))
			}
		})
	}
}

func TestParseMessageHistoryBundleMessage(t *testing.T) {
	wa := &WhatsAppClient{Client: &whatsmeow.Client{}}
	group := types.NewJID("120363000000000000", types.GroupServer)
	sharer := types.NewJID("333", types.HiddenUserServer)
	bundleEvt := &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: group, Sender: sharer, IsGroup: true},
		PushName:      "Sharer",
	}}

	evt, err := wa.parseMessageHistoryBundleMessage(bundleEvt, testWebMessage("A", 100, false, "222@lid", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	if evt.Info.IsFromMe || evt.Info.Sender.String() != "222@lid" || evt.Info.Chat != group {
		t.Errorf("member's message parsed as %+v", evt.Info.MessageSource)
	}

	ownMsg := testWebMessage("B", 200, true, "", "mine")
	evt, err = wa.parseMessageHistoryBundleMessage(bundleEvt, ownMsg)
	if err != nil {
		t.Fatal(err)
	}
	if evt.Info.IsFromMe || evt.Info.Sender != sharer || evt.Info.PushName != "Sharer" {
		t.Errorf("sharer's own message parsed as %+v (push name %q)", evt.Info.MessageSource, evt.Info.PushName)
	}
	if !ownMsg.GetKey().GetFromMe() {
		t.Error("parsing changed the bundle's own copy of the message")
	}

	other := testWebMessage("C", 300, false, "222@lid", "elsewhere")
	other.Key.RemoteJID = proto.String("120363999999999999@g.us")
	if _, err = wa.parseMessageHistoryBundleMessage(bundleEvt, other); err == nil {
		t.Error("accepted a message from another chat")
	}
}

func TestIsSharedWithMe(t *testing.T) {
	wa := &WhatsAppClient{
		JID: types.NewADJID("48000000000", 0, 24),
		LID: types.NewADJID("31100000000000", 0, 24),
	}
	notice := func(receivers ...string) *waE2E.MessageHistoryNotice {
		return &waE2E.MessageHistoryNotice{MessageHistoryMetadata: &waE2E.MessageHistoryMetadata{HistoryReceivers: receivers}}
	}
	if !wa.isSharedWithMe(notice("222@lid", "31100000000000@lid")) {
		t.Error("missed our LID")
	}
	if !wa.isSharedWithMe(notice("48000000000@s.whatsapp.net")) {
		t.Error("missed our phone number")
	}
	if wa.isSharedWithMe(notice("222@lid", "333@s.whatsapp.net")) || wa.isSharedWithMe(notice()) {
		t.Error("claimed a share with others")
	}
}

func TestSharedHistoryReaction(t *testing.T) {
	group := types.NewJID("120363000000000000", types.GroupServer)
	own := types.JID{User: "31100000000000", Device: 24, Server: types.HiddenUserServer}
	target := types.NewJID("222", types.HiddenUserServer)
	reaction := func(fromMe bool, participant, text string) *waWeb.Reaction {
		key := &waCommon.MessageKey{FromMe: proto.Bool(fromMe), ID: proto.String("R")}
		if participant != "" {
			key.Participant = proto.String(participant)
		}
		return &waWeb.Reaction{Key: key, Text: proto.String(text), SenderTimestampMS: proto.Int64(1700000000000)}
	}

	evt := sharedHistoryReaction(group, own, true, target, "A", reaction(false, "333@lid", "👍"))
	if evt == nil || evt.Info.Sender.String() != "333@lid" || evt.Info.IsFromMe || evt.Info.ID != "R" {
		t.Fatalf("member's reaction became %+v", evt)
	}
	key := evt.Message.GetReactionMessage().GetKey()
	if key.GetID() != "A" || key.GetParticipant() != "222@lid" || key.GetFromMe() || key.GetRemoteJID() != group.String() {
		t.Errorf("reaction targets %+v", key)
	}
	if getMessageType(evt.Message) != "reaction" {
		t.Errorf("parsed as %s", getMessageType(evt.Message))
	}

	evt = sharedHistoryReaction(group, own, true, target, "A", reaction(true, "", "❤️"))
	if evt == nil || evt.Info.Sender.String() != "31100000000000@lid" || !evt.Info.IsFromMe {
		t.Errorf("own reaction became %+v", evt)
	}
	evt = sharedHistoryReaction(group, target, false, target, "A", reaction(true, "", "❤️"))
	if evt == nil || evt.Info.Sender != target || evt.Info.IsFromMe {
		t.Errorf("sharer's reaction became %+v", evt)
	}
	if sharedHistoryReaction(group, own, true, target, "A", reaction(false, "333@lid", "")) != nil {
		t.Error("kept a removed reaction")
	}
	if sharedHistoryReaction(group, own, true, target, "A", reaction(false, "", "👍")) != nil {
		t.Error("kept a reaction from nobody")
	}
}
