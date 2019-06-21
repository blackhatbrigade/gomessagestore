package gomessagestore

import (
	"context"
	"strings"
)

//Subscriber allows for reaching out to the message service on a continual basis
type Subscriber interface {
	Start(context.Context) error
}

type subscriber struct {
	config       *SubscriberConfig
	poller       Poller
	ms           MessageStore
	handlers     []MessageHandler
	subscriberID string
}

//CreateSubscriber
func (ms *msgStore) CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) {

	subscriber := &subscriber{
		ms:           ms,
		handlers:     handlers,
		subscriberID: subscriberID,
	}

	//Validate the params
	if handlers == nil {
		return nil, ErrSubscriberMessageHandlersEqualToNil
	}
	for _, handler := range handlers {
		if handler == nil {
			return nil, ErrSubscriberMessageHandlerEqualToNil
		}
	}
	if len(handlers) < 1 {
		return nil, ErrSubscriberNeedsAtLeastOneMessageHandler
	}
	if subscriberID == "" {
		return nil, ErrSubscriberIDCannotBeEmpty
	}
	if strings.Contains(subscriberID, "-") {
		return nil, ErrInvalidSubscriberID
	}
	if strings.Contains(subscriberID, "+") {
		return nil, ErrInvalidSubscriberID
	}

	if config, err := GetSubscriberConfig(opts...); err != nil {
		return nil, err
	} else {
		subscriber.config = config
	}

	worker, err := CreateWorker(
		subscriber.ms,
		subscriberID,
		subscriber.handlers,
		subscriber.config)
	if err != nil {
		return nil, err
	}

	subscriber.poller, err = CreatePoller(subscriber.ms, worker, subscriber.config)

	return subscriber, nil
}
