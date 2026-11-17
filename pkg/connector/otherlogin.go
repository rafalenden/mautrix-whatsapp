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

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

// otherLoginInGroup returns another of the user's logins to send with, after
// WhatsApp said this one isn't in the group.
//
// A group's portal is shared by all of a user's logins, and the bridge sends
// with the first one recorded as being in it. That record can be wrong, and
// then everything the user sends there fails although another of their logins
// is a member. So this login's record is dropped, which makes later events go
// straight to the other one. It's kept if it's the only one, as the user has
// then simply left the group.
func (wa *WhatsAppClient) otherLoginInGroup(ctx context.Context, portal *bridgev2.Portal, sendErr error) *WhatsAppClient {
	if !errors.Is(sendErr, whatsmeow.ErrNotInGroup) || portal == nil || portal.Receiver != "" {
		return nil
	}
	log := zerolog.Ctx(ctx)
	userPortals := wa.Main.Bridge.DB.UserPortal
	rows, err := userPortals.GetAllForUserInPortal(ctx, wa.UserLogin.UserMXID, portal.PortalKey)
	if err != nil {
		log.Err(err).Msg("Failed to get the user's logins in portal after not in group error")
		return nil
	}
	var own *database.UserPortal
	hasOther := false
	for _, row := range rows {
		if row.LoginID == wa.UserLogin.ID {
			own = row
		} else {
			hasOther = true
		}
	}
	if !hasOther {
		return nil
	}
	if own != nil {
		if err = userPortals.Delete(ctx, own); err != nil {
			log.Err(err).Msg("Failed to forget login that isn't in the group")
			return nil
		}
	}
	login, _, err := portal.FindPreferredLogin(ctx, wa.UserLogin.User, false)
	if err != nil || login == nil || login.ID == wa.UserLogin.ID {
		log.Debug().Err(err).Msg("No other login to send with after not in group error")
		return nil
	}
	other, ok := login.Client.(*WhatsAppClient)
	if !ok || other.Client == nil {
		return nil
	}
	log.Warn().
		Str("other_login_id", string(login.ID)).
		Msg("Login isn't in the group, sending with the user's other login instead")
	return other
}
