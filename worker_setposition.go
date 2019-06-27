package gomessagestore

import (
	"context"

	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//SetPosition sets the position of a subscriber
func (sw *subscriptionWorker) SetPosition(ctx context.Context, position int64) error {
	newUUID := uuid.NewRandom()

	//only needs these three fields for writing
	var posMsg Message
	posMsg = &positionMessage{
		ID:           newUUID,
		MyPosition:   position,
		SubscriberID: sw.subscriberID,
	}

	return sw.ms.Write(
		ctx,
		posMsg,
	)
}
