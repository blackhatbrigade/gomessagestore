package gomessagestore

import (
	"context"
	"fmt"
	"time"
)

//Subscriber allows for reaching out to the message service on a continual basis
type Subscriber interface {
	Start(*context.Context) error
}

type subscriber struct {
	ms       MessageStore
	handlers []MessageHandler
	stream   string
	category string
	pollTime time.Duration
}

//SubscriberOption allows for various options when creating a subscriber
type SubscriberOption func(sub *subscriber) error

//CreateSubscriber
func (ms *msgStore) CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) {

	subscriber := &subscriber{
		ms:       ms,
		pollTime: 200 * time.Millisecond,
	}

	for _, option := range opts {
		err := option(subscriber)
		if err != nil {
			return nil, err
		}
	}

	//Validate the params
	if subscriber.stream != "" && subscriber.category != "" {
		return nil, ErrSubscriberCannotUseBothStreamAndCategory
	}
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
	if subscriber.stream == "" && subscriber.category == "" {
		return nil, ErrSubscriberNeedsCategoryOrStream
	}
	if subscriber.pollTime <= 0 {
		return nil, ErrInvalidPollTime
	}

	return subscriber, nil
}

//UpdatePollTime
func (sub *subscriber) UpdatePollTime(pollTime time.Duration) error {
	return nil
}

//Start
func (sub *subscriber) Start(ctx *context.Context) error {

	return nil
}

//Subscribe to a specific entity stream
func SubscribeToEntityStream(category, entityID string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.stream != "" {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if category != "" && entityID != "" {
			sub.stream = fmt.Sprintf("%s-%s", category, entityID)
		}
		return nil
	}
}

//Subscribe to a specific command stream
func SubscribeToCommandStream(category string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.stream != "" {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if category != "" {
			sub.stream = fmt.Sprintf("%s:command", category)
		}
		return nil
	}
}

//Subscribe to a category of streams
func SubscribeToCategory(category string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.category != "" {
			return ErrSubscriberCannotSubscribeToMultipleCategories
		}
		sub.category = category
		return nil
	}
}

func PollTime(pollTime time.Duration) SubscriberOption {
	return func(sub *subscriber) error {
		sub.pollTime = pollTime
		return nil
	}
}
