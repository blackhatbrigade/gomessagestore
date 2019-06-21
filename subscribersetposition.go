package gomessagestore

import (
	"context"
)

//SetPosition sets the position of messages it recieves
func (sub *subscriber) SetPosition(ctx context.Context, msgs []Message) error {
	return nil
}
