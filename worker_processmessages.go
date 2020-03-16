package gomessagestore

import (
	"context"

	"github.com/sirupsen/logrus"
)

//ProcessMessages uses the handlers of the subscriptionWorker to process the messages retrieved from the message store; third process of the polling loop
func (sw *subscriptionWorker) ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error) {
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
	return
}
