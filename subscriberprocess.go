package gomessagestore

import (
	"context"
)

//ProcessMessages phase three
func (sub *subscriber) ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error) {

	for _, msg := range msgs {
		for _, handler := range sub.handlers {
			if handler.Type() == msg.Type() {
				if err = handler.Process(ctx, msg); err != nil {
					return
				}
				messagesHandled++
				if sub.entityID == "" {
					// category subscriptions care about position
					positionOfLastHandled = msg.Position()
				} else {
					// stream subscriptions care about version
					positionOfLastHandled = msg.Version()
				}
			}
		}
	}
	return
}
