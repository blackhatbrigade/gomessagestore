package gomessagestore

import (
	"context"

	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//SetPosition sets the position of messages it recieves
func (sw *subscriptionWorker) SetPosition(ctx context.Context, msg Message) error {
	newUUID := uuid.NewRandom()

	//only needs these three fields for writing
	var posMsg Message
	if sw.config.stream {
		posMsg = &positionMessage{
			ID:           newUUID,
			MyPosition:   msg.Version(),
			SubscriberID: sw.subscriberID,
		}
	} else {
		posMsg = &positionMessage{
			ID:           newUUID,
			MyPosition:   msg.Position(),
			SubscriberID: sw.subscriberID,
		}
	}

	return sw.ms.Write(
		ctx,
		posMsg,
	)
}
