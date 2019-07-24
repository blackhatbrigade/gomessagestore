package gomessagestore

import (
	"context"
	"strings"
)

// go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Subscriber > mocks/subscriber.go"

// Subscriber allows for reaching out to the message service on a continual basis
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

// CreateSubscriber creates a new Subscriber
func (ms *msgStore) CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) {
	subscriber, err := createSubscriberWithPoller(
		ms,
		subscriberID,
		handlers,
		nil,
		opts...,
	)
	if err != nil {
		return nil, err
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
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

// CreateSubscriberWithPoller is used for testing with dependency injection
func CreateSubscriberWithPoller(ms MessageStore, subscriberID string, handlers []MessageHandler, poller Poller, opts ...SubscriberOption) (Subscriber, error) {
	return createSubscriberWithPoller(
		ms,
		subscriberID,
		handlers,
		poller,
		opts...,
	)
}

// createSubscriberWithPoller creates a new subscriber with a provided Poller
func createSubscriberWithPoller(ms MessageStore, subscriberID string, handlers []MessageHandler, poller Poller, opts ...SubscriberOption) (*subscriber, error) {

	subscriber := &subscriber{
		ms:           ms,
		handlers:     handlers,
		subscriberID: subscriberID,
		poller:       poller,
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

	return subscriber, nil
}
