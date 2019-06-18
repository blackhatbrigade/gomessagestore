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
	opts := []GetOption{
		Since(0),
	}
	if sub.entityID == "" {
		opts = append(opts, Category(sub.category))
	} else {
		if sub.commandCategory != "" {
			opts = append(opts, CommandStream(sub.commandCategory))
		} else {
			opts = append(opts, EventStream(sub.category, sub.entityID))
		}
	}

	sub.ms.Get(
		ctx,
		PositionStream(sub.subscriberID),
		Last(),
	)
	msgs, _ := sub.ms.Get(ctx, opts...)

	for _, msg := range msgs {
		if sub.handlers[0].Type() == msg.Type() {
			sub.handlers[0].Process(ctx, msg)
		}
	}

	return nil
}

//func (sub *subscriber) grabMessages(ctx context.Context) ([]Message, error) {
//	return sub.ms.Get(ctx, opts...)
//}
