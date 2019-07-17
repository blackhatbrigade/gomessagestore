package gomessagestore

import (
	"context"

	"github.com/sirupsen/logrus"
)

//ProcessMessages phase three
func (sw *subscriptionWorker) ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error) {
	log := logrus.WithFields(logrus.Fields{
		"subscriberID": sw.subscriberID,
	})

	for _, msg := range msgs {
		for _, handler := range sw.handlers {
			if handler.Type() == msg.Type() {
				processErr := handler.Process(ctx, msg)
				if processErr != nil {
					log.WithError(processErr).Error("A handler failed to process a message, moving on")
				}

				if !sw.config.stream {
					// category subscriptions care about position
					positionOfLastHandled = msg.Position()
				} else {
					// stream subscriptions care about version
					positionOfLastHandled = msg.Version()
				}
				messagesHandled++
			}
		}
	}
	return
}
