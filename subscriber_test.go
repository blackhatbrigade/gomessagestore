package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestCreateSubscriber(t *testing.T) {

	messageHandler := struct{}{}

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
		name:          "when subscriberID is nil",
		expectedError: ErrSubscriberIDCannotBeEmpty,
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

	messageHandler := struct{}{}

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
			SubscribeToEntityStream("some stream", "some id"),
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
			SubscribeToEntityStream("", "some id"),
		},
	}, {
		name:          "Subscribe to entity stream, entityID cannot be blank",
		expectedError: ErrSubscriberNeedsCategoryOrStream,
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", ""),
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
			SubscribeToEntityStream("some category", "some id"),
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
		name:          "Cannot set negative poll time",
		expectedError: ErrInvalidPollTime,
		opts: []SubscriberOption{
			PollTime(-100),
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

func TestSubscriberStart(t *testing.T) {

	messageHandler := struct{}{}

	tests := []struct {
		name             string
		subscriberID     string
		expectedError    error
		handlers         []MessageHandler
		expectedPosition int64
		expectedStream   string
		opts             []SubscriberOption
		messageEnvelopes []*repository.MessageEnvelope
		repoReturnError  error
	}{{
		name:           "Repository is called when Start method is invoked",
		subscriberID:   "some id",
		expectedStream: "some category-some id1",
		handlers:       []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "some id1"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

			mockRepo.
				EXPECT().
				GetAllMessagesInStreamSince(ctx, test.expectedStream, test.expectedPosition).
				Return(test.messageEnvelopes, test.repoReturnError)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			mySubscriber, err := myMessageStore.CreateSubscriber(
				"someid",
				[]MessageHandler{messageHandler},
				test.opts...,
			)

			if err != nil {
				t.Errorf("Failed on CreateSubscriber() Got: %s\n", err)
				return
			}

			err = mySubscriber.Start(ctx)
			if err != test.expectedError {
				t.Errorf("Failed to get expected error from Start()\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}
		})
	}
}
