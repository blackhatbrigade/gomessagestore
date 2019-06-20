package gomessagestore

import (
	"context"
)

//Start Handles polling at specified intervals
func (sub *subscriber) Start(ctx context.Context) error {
	return sub.Poll(ctx)
}

//Poll Handles a single tick of the handlers firing
func (sub *subscriber) Poll(ctx context.Context) error {
	pos, err := sub.GetPosition(ctx)
	if err != nil {
		return err
	}

	msgs, err := sub.GetMessages(ctx, pos)
	if err != nil {
		return err
	}

	_, _, err = sub.ProcessMessages(ctx, msgs)

	return err
}
