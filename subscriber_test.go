package gomessagestore_test

import (
	"context"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
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

type msgHandler struct {
	Called  bool
	Handled []string
}

func (mh *msgHandler) Process(ctx context.Context, msg Message) error {
	mh.Called = true
	if mh.Handled == nil {
		mh.Handled = []string{msg.Type()}
	} else {
		mh.Handled = append(mh.Handled, msg.Type())
	}
	return nil
}

func TestSubscriberStart(t *testing.T) {
	messageHandler := &msgHandler{}

	tests := []struct {
		name                string
		subscriberID        string
		expectedError       error
		handlers            []MessageHandler
		expectedPosition    int64
		expectedStream      string
		expectedCategory    string
		opts                []SubscriberOption
		messageEnvelopes    []*repository.MessageEnvelope
		repoReturnError     error
		expectedHandlerBool bool
		expectedHandled     []string
	}{{
		name:           "Repository is called for a stream when Start method is invoked",
		subscriberID:   "some id",
		expectedStream: "some category-some id1",
		handlers:       []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "some id1"),
		},
	}, {
		name:             "Repository is called for a category when Start method is invoked",
		subscriberID:     "some id",
		expectedCategory: "some category",
		handlers:         []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
		},
	}, {
		name:           "Repository is called for a command stream  when Start method is invoked",
		handlers:       []MessageHandler{messageHandler},
		expectedStream: "some category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
		},
	}, {
		name:                "Subscriber Start processes a message in the registered handler",
		handlers:            []MessageHandler{&msgHandler{}},
		expectedHandlerBool: true,
		expectedHandled:     []string{"Command MessageType 1"},
		expectedStream:      "category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("category"),
		},
		messageEnvelopes: getSampleCommandsAsEnvelopes(),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

			if test.expectedStream != "" {
				mockRepo.
					EXPECT().
					GetAllMessagesInStreamSince(ctx, test.expectedStream, test.expectedPosition).
					Return(test.messageEnvelopes, test.repoReturnError)
			}
			if test.expectedCategory != "" {
				mockRepo.
					EXPECT().
					GetAllMessagesInCategorySince(ctx, test.expectedCategory, test.expectedPosition).
					Return(test.messageEnvelopes, test.repoReturnError)
			}

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			mySubscriber, err := myMessageStore.CreateSubscriber(
				"some id",
				test.handlers,
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

			if test.expectedHandlerBool {
				switch handler := test.handlers[0].(type) {
				case *msgHandler:
					if !handler.Called {
						t.Error("Handler was not called")
					}
					if !reflect.DeepEqual(handler.Handled, test.expectedHandled) {
						t.Errorf("Handler was called for the wrong messages, \nCalled: %s\nExpected: %s\n", handler.Handled, test.expectedHandled)
					}
				default:
					t.Errorf("Invalid type found %T", test.handlers[0])
				}
			}
		})
	}
}
