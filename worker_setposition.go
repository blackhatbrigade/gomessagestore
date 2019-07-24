package gomessagestore

import (
	"context"

	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//SetPosition sets the position of a subscriber; fourth process in the polling loop after all messages have been handled
func (sw *subscriptionWorker) SetPosition(ctx context.Context, position int64) error {
	newUUID := uuid.NewRandom()

	var posMsg Message
	// Create and write a positionMessage so we can track how the position changes over time
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
