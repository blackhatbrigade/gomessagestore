package gomessagestore

import (
	"context"

	"github.com/google/uuid"
)

//SetPosition sets the position of messages it recieves
func (sw *subscriptionWorker) SetPosition(ctx context.Context, msg Message) error {
	newUUID, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	//only needs these three fields for writing
	posMsg := &positionMessage{
		ID:           newUUID.String(),
		MyPosition:   msg.Position(),
		SubscriberID: sw.subscriberID,
	}

	sw.ms.Write(
		ctx,
		posMsg,
	)

	return nil
}
