package gomessagestore

import (
	"context"
)

//SetPosition sets the position of messages it recieves
func (sw *subscriptionWorker) SetPosition(ctx context.Context, msgs []Message) error {
	return nil
}
