package gomessagestore_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestCreateSubscriber(t *testing.T) {

	messageHandler := &msgHandler{}

	tests := []struct {
		name          string
		subscriberID  string
		expectedError error
		handlers      []MessageHandler
	}{{
		name:          "when given an empty list of handlers",
		subscriberID:  "someid1",
		expectedError: ErrSubscriberNeedsAtLeastOneMessageHandler,
		handlers:      []MessageHandler{},
	}, {
		name:          "when subscriberID is blank",
		expectedError: ErrSubscriberIDCannotBeEmpty,
		handlers:      []MessageHandler{messageHandler},
	}, {
		name:          "when subscriberID contains a hyphen",
		subscriberID:  "someid1-plus a hyphen",
		expectedError: ErrInvalidSubscriberID,
		handlers:      []MessageHandler{messageHandler},
	}, {
		name:          "when subscriberID contains a plus",
		subscriberID:  "someid1+a plus",
		expectedError: ErrInvalidSubscriberID,
		handlers:      []MessageHandler{messageHandler},
	}, {
		name:          "messageHandler is equal to nil",
		subscriberID:  "someid1",
		expectedError: ErrSubscriberMessageHandlersEqualToNil,
	}, {
		name:          "individual messageHandlers cannot equal nil",
		subscriberID:  "someid1",
		expectedError: ErrSubscriberMessageHandlerEqualToNil,
		handlers:      []MessageHandler{messageHandler, nil},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := mock_repository.NewMockRepository(ctrl)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			_, err := myMessageStore.CreateSubscriber(
				test.subscriberID,
				test.handlers,
			)

			if err != test.expectedError {
				t.Errorf("Failed to get expected error from CreateSubscriber()\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}
		})
	}
}

func TestCreateSubscriberOptions(t *testing.T) {

	messageHandler := &msgHandler{}

	tests := []struct {
		name          string
		expectedError error
		opts          []SubscriberOption
	}{{
		name:          "category or stream needs to be set",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
	}, {
		name:          "both category and stream cannot be set",
		expectedError: ErrSubscriberCannotUseBothStreamAndCategory,
		opts: []SubscriberOption{
			SubscribeToEntityStream("some stream", uuid1),
			SubscribeToCategory("some category"),
		},
	}, {
		name: "Subscribe to command stream does not return error",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
		},
	}, {
		name:          "Subscribe to command stream category cannot be blank",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
		opts: []SubscriberOption{
			SubscribeToCommandStream(""),
		},
	}, {
		name:          "Subscribe to entity stream category cannot be blank",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
		opts: []SubscriberOption{
			SubscribeToEntityStream("", uuid1),
		},
	}, {
		name:          "Subscribe to entity stream, entityID cannot be blank",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", NilUUID),
		},
	}, {
		name:          "Subscribe to category stream, category cannot be blank",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
		opts: []SubscriberOption{
			SubscribeToCategory(""),
		},
	}, {
		name:          "Subscribe should only accept one subscription request, (command and entity)",
		expectedError: ErrSubscriberCannotSubscribeToMultipleStreams,
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
			SubscribeToEntityStream("some category", uuid1),
		},
	}, {
		name:          "Subscribe should only accept one category subscription request, (command and entity)",
		expectedError: ErrSubscriberCannotSubscribeToMultipleCategories,
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Cannot set 0 poll time",
		expectedError: ErrInvalidPollTime,
		opts: []SubscriberOption{
			PollTime(0),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Cannot set 0 poll error delay",
		expectedError: ErrInvalidPollErrorDelay,
		opts: []SubscriberOption{
			PollErrorDelay(0),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Cannot set negative poll time",
		expectedError: ErrInvalidPollTime,
		opts: []SubscriberOption{
			PollTime(-100),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Cannot set negative poll error delay",
		expectedError: ErrInvalidPollErrorDelay,
		opts: []SubscriberOption{
			PollErrorDelay(-100),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Update position cannot be less than 2 for msgInterval",
		expectedError: ErrInvalidMsgInterval,
		opts: []SubscriberOption{
			UpdatePositionEvery(1),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Batch size cannot be zero",
		expectedError: ErrInvalidBatchSize,
		opts: []SubscriberOption{
			SubscribeBatchSize(0),
			SubscribeToCategory("some category"),
		},
	}, {
		name:          "Options cannot include a nil option",
		expectedError: ErrSubscriberNilOption,
		opts: []SubscriberOption{
			nil,
		},
	}, {
		name:          "Batch size cannot be negative",
		expectedError: ErrInvalidBatchSize,
		opts: []SubscriberOption{
			SubscribeBatchSize(-1),
			SubscribeToCategory("some category"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := mock_repository.NewMockRepository(ctrl)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			_, err := myMessageStore.CreateSubscriber(
				"someid",
				[]MessageHandler{messageHandler},
				test.opts...,
			)

			if err != test.expectedError {
				t.Errorf("Failed to get expected error from CreateSubscriber()\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}
		})
	}
}

type msgHandler struct {
	called  bool
	handled []string
	class   string
	retErr  error
}

func (mh *msgHandler) Type() string {
	return mh.class
}

func (mh *msgHandler) Process(ctx context.Context, msg Message) error {
	mh.called = true
	if mh.retErr != nil {
		return mh.retErr
	}
	switch msg.(type) {
	case *Event:
		mh.class = msg.Type()
		mh.handled = append(mh.handled, mh.class)
	case *Command:
		mh.class = msg.Type()
		mh.handled = append(mh.handled, mh.class)
	default:
		return errors.New("something weird got handed to me")
	}
	return nil
}
