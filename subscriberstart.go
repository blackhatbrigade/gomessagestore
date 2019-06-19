package gomessagestore

import (
	"context"

	"github.com/sirupsen/logrus"
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

//GetPosition starts phase one of the polling loop
func (sub *subscriber) GetPosition(ctx context.Context) (int64, error) {
	log := logrus.
		WithFields(logrus.Fields{
			"SubscriberID": sub.subscriberID,
		})

	msgs, _ := sub.ms.Get(
		ctx,
		PositionStream(sub.subscriberID),
		Converter(convertEnvelopeToPositionMessage),
		Last(),
	)
	if len(msgs) < 1 {
		log.Debug("no messages found for subscriber, using default")
		return 0, nil
	}

	switch pos := msgs[0].(type) {
	case *positionMessage:
		return pos.Position, nil
	default:
		log.
			WithError(ErrIncorrectMessageInPositionStream).
			Error("incorrect message type in position stream")
		return 0, nil
	}
}

//GetMessages phase two
func (sub *subscriber) GetMessages(ctx context.Context, position int64) ([]Message, error) {
	opts := []GetOption{
		Since(position),
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

	return sub.ms.Get(ctx, opts...)
}

//ProcessMessages phase three
func (sub *subscriber) ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error) {

	for _, msg := range msgs {
		if sub.handlers[0].Type() == msg.Type() {
			sub.handlers[0].Process(ctx, msg)
		}
	}
	return
}

//SetPosition phase four
func (sub *subscriber) SetPosition(ctx context.Context, msgs []Message) error {
	return nil
}
