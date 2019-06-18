package gomessagestore

import (
	"context"
	"time"
)

//Subscriber allows for reaching out to the message service on a continual basis
type Subscriber interface {
	Start(context.Context) error
}

type subscriber struct {
	ms              MessageStore
	handlers        []MessageHandler
	entityID        string
	category        string
	commandCategory string
	pollTime        time.Duration
	updateInterval  int
	batchSize       int
}

//SubscriberOption allows for various options when creating a subscriber
type SubscriberOption func(sub *subscriber) error

//CreateSubscriber
func (ms *msgStore) CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) {

	subscriber := &subscriber{
		ms:             ms,
		pollTime:       200 * time.Millisecond,
		updateInterval: 100,
		handlers:       handlers,
	}

	for _, option := range opts {
		if option == nil {
			return nil, ErrSubscriberNilOption
		}
		err := option(subscriber)
		if err != nil {
			return nil, err
		}
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
	if subscriber.entityID == "" && subscriber.category == "" {
		return nil, ErrSubscriberNeedsCategoryOrStream
	}
	if subscriber.pollTime <= 0 {
		return nil, ErrInvalidPollTime
	}
	if subscriber.updateInterval < 2 {
		return nil, ErrInvalidMsgInterval
	}

	return subscriber, nil
}

//UpdatePollTime
func (sub *subscriber) UpdatePollTime(pollTime time.Duration) error {
	return nil
}

//Start
func (sub *subscriber) Start(ctx context.Context) error {
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
	msgs, _ := sub.ms.Get(ctx, opts...)

	for _, msg := range msgs {
		sub.handlers[0].Process(ctx, msg)
	}

	return nil
}

//Subscribe to a specific entity stream
func SubscribeToEntityStream(category, entityID string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.entityID != "" {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if category != "" && entityID != "" {
			sub.entityID = entityID
			sub.category = category
		}
		return nil
	}
}

//Subscribe to a specific command stream
func SubscribeToCommandStream(category string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.entityID != "" {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if sub.category != "" {
			return ErrSubscriberCannotUseBothStreamAndCategory
		}
		if category != "" {
			sub.commandCategory = category
			sub.entityID = "none"
		}
		return nil
	}
}

//Subscribe to a category of streams
func SubscribeToCategory(category string) SubscriberOption {
	return func(sub *subscriber) error {
		if sub.entityID != "" {
			return ErrSubscriberCannotUseBothStreamAndCategory
		}
		if sub.category != "" {
			return ErrSubscriberCannotSubscribeToMultipleCategories
		}
		sub.category = category
		return nil
	}
}

//PollTime sets the interval between handling operations
func PollTime(pollTime time.Duration) SubscriberOption {
	return func(sub *subscriber) error {
		sub.pollTime = pollTime
		return nil
	}
}

//UpdatePostionEvery updates position of subscriber based on a msgInterval (cannot be < 2)
//An interval of 1 would create an event on every message, and possibly be picked up by itself, creating another event, and so on
func UpdatePositionEvery(msgInterval int) SubscriberOption {
	return func(sub *subscriber) error {
		sub.updateInterval = msgInterval
		return nil
	}
}

//SubscribeBatchSize sets the amount of messages to retrieve in a single handling operation
func SubscribeBatchSize(batchSize int) SubscriberOption {
	return func(sub *subscriber) error {
		if batchSize < 1 {
			return ErrInvalidBatchSize
		}
		sub.batchSize = batchSize
		return nil
	}
}
