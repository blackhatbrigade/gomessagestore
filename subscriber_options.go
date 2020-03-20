package gomessagestore

import (
	"time"

	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

//SubscriberOption allows for various options when creating a subscriber
type SubscriberOption func(config *SubscriberConfig) error

// SubscriberConfig contains configuration information for a subscriber
type SubscriberConfig struct {
	entityID        uuid.UUID
	stream          bool
	category        string
	commandCategory string
	pollTime        time.Duration // the time interval between polling operations
	pollErrorDelay  time.Duration // the time interval to wait after an error occurs during a poll operation
	updateInterval  int           //
	batchSize       int           // the maximum amount of messages to be retrieved at a time
	position        int64         // the position from which to retrieve messages
	log             logrus.FieldLogger
	converters      []MessageConverter // convert non-command/event messages
	errorFunc       func(error)
}

//SubscribeToEntityStream subscribes to a specific entity stream and ensures that multiple streams are not subscribed to
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

//SubscribeToCommandStream subscribes to a specific command stream and ensures that multiple streams are not subscribed to
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

//SubscribeToCategory subscribes to a category of streams and ensures that it is not also subscribed to a stream
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

// PollTime sets the interval between handling operations
func PollTime(pollTime time.Duration) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.pollTime = pollTime
		return nil
	}
}

// PollErrorDelay sets the interval between handling operations when Poll() errors
func PollErrorDelay(pollErrorDelay time.Duration) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.pollErrorDelay = pollErrorDelay
		return nil
	}
}

// UpdatePositionEvery determines how often a positionMessage is written to the message store to save the position of the worker; must be >= 2
func UpdatePositionEvery(msgInterval int) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.updateInterval = msgInterval
		return nil
	}
}

// SubscribeBatchSize sets the amount of messages to retrieve in a single handling operation
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
	if config.log == nil {
		config.log = logrus.New()
	}

	return config, nil
}

// SubscribeLogger allows to configure the logger used inside the Subscriber
func SubscribeLogger(logger logrus.FieldLogger) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.log = logger
		return nil
	}
}

// OnError when the subscriber reaches an error, it will call this func instead of panicking
func OnError(errorFunc func(error)) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.errorFunc = errorFunc
		return nil
	}
}

//WithConverter allows for automatic converting of non-Command/Event type messages
func WithConverter(converter MessageConverter) SubscriberOption {
	return func(sub *SubscriberConfig) error {
		sub.converters = append(sub.converters, converter)
		return nil
	}
}
