package gomessagestore

import (
	"time"

	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//SubscriberOption allows for various options when creating a subscriber
type SubscriberOption func(config *SubscriberConfig) error

type SubscriberConfig struct {
	entityID        uuid.UUID
	stream          bool
	category        string
	commandCategory string
	pollTime        time.Duration
	pollErrorDelay  time.Duration
	updateInterval  int
	batchSize       int
	position        int64
}

//Subscribe to a specific entity stream
func SubscribeToEntityStream(category string, entityID uuid.UUID) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		if sub.stream {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if category != "" && entityID != NilUUID {
			sub.entityID = entityID
			sub.category = category
			sub.stream = true
		}
		return nil
	}
}

//Subscribe to a specific command stream
func SubscribeToCommandStream(category string) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		if sub.stream {
			return ErrSubscriberCannotSubscribeToMultipleStreams
		}
		if sub.category != "" {
			return ErrSubscriberCannotUseBothStreamAndCategory
		}
		if category != "" {
			sub.commandCategory = category
			sub.stream = true
		}
		return nil
	}
}

//Subscribe to a category of streams
func SubscribeToCategory(category string) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		if sub.stream {
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
	return func(sub *SubscriberConfig) error {
		sub.pollTime = pollTime
		return nil
	}
}

//PollErrorDelay sets the interval between handling operations when Poll() errors
func PollErrorDelay(pollErrorDelay time.Duration) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.pollErrorDelay = pollErrorDelay
		return nil
	}
}

//UpdatePostionEvery updates position of subscriber based on a msgInterval (cannot be < 2)
//An interval of 1 would create an event on every message, and possibly be picked up by itself, creating another event, and so on
func UpdatePositionEvery(msgInterval int) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.updateInterval = msgInterval
		return nil
	}
}

//SubscribeBatchSize sets the amount of messages to retrieve in a single handling operation
func SubscribeBatchSize(batchSize int) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		if batchSize < 1 {
			return ErrInvalidBatchSize
		}
		sub.batchSize = batchSize
		return nil
	}
}

//GetSubscriberConfig changes SubscriberOptions into a valid SubscriberConfig object, or returns an error
func GetSubscriberConfig(opts ...SubscriberOption) (*SubscriberConfig, error) {
	config := &SubscriberConfig{
		pollTime:       200 * time.Millisecond,
		pollErrorDelay: 5 * time.Second,
		updateInterval: 100,
	}
	for _, option := range opts {
		if option == nil {
			return nil, ErrSubscriberNilOption
		}
		if err := option(config); err != nil {
			return nil, err
		}
	}

	if !config.stream && config.category == "" {
		return nil, ErrSubscriberNeedsCategoryOrStream
	}
	if config.pollTime <= 0 {
		return nil, ErrInvalidPollTime
	}
	if config.pollErrorDelay <= 0 {
		return nil, ErrInvalidPollErrorDelay
	}
	if config.updateInterval < 2 {
		return nil, ErrInvalidMsgInterval
	}

	return config, nil
}
