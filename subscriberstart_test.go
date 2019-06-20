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

func TestSubscriberGetsMessages(t *testing.T) {
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
		positionEnvelope    *repository.MessageEnvelope
	}{{
		name:           "When subscriber is called with SubscribeToEntityStream() option, repository is called correctly",
		subscriberID:   "some id",
		expectedStream: "some category-some id1",
		handlers:       []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "some id1"),
		},
	}, {
		name:             "When subscriber is called with SubscribeToCategory() option, repository is called correctly",
		subscriberID:     "some id",
		expectedCategory: "some category",
		handlers:         []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
		},
	}, {
		name:           "When subscriber is called with SubscribeToEntityStream() option, repository is called correctly",
		handlers:       []MessageHandler{messageHandler},
		expectedStream: "some category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
		},
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
					GetAllMessagesInStreamSince(ctx, test.expectedStream, test.expectedPosition, 1000).
					Return(test.messageEnvelopes, test.repoReturnError)
			}
			if test.expectedCategory != "" {
				mockRepo.
					EXPECT().
					GetAllMessagesInCategorySince(ctx, test.expectedCategory, test.expectedPosition, 1000).
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

			_, err = mySubscriber.GetMessages(ctx, test.expectedPosition)
			if err != test.expectedError {
				t.Errorf("Failed to get expected error from GetMessages()\nExpected: %s\n and got: %s\n", test.expectedError, err)
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

func TestSubscriberProcessesMessages(t *testing.T) {

	tests := []struct {
		name                string
		subscriberID        string
		expectedError       error
		handlers            []MessageHandler
		expectedPosition    int64
		expectedStream      string
		expectedCategory    string
		opts                []SubscriberOption
		messages            []Message
		repoReturnError     error
		expectedHandlerBool bool
		expectedHandled     []string
		positionEnvelope    *repository.MessageEnvelope
	}{{
		name:                "Subscriber Poll processes a message in the registered handler",
		handlers:            []MessageHandler{&msgHandler{}},
		expectedHandlerBool: true,
		expectedHandled:     []string{"Command MessageType 1"},
		expectedStream:      "category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("category"),
		},
		messages: commandsToMessageSlice(getSampleCommands()),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

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

			_, _, err = mySubscriber.ProcessMessages(ctx, test.messages)
			if err != test.expectedError {
				t.Errorf("Failed to get expected error from Poll()\nExpected: %s\n and got: %s\n", test.expectedError, err)
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

func TestSubscriberGetsPosition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var expectedPos int64 = 0

	handlers := []MessageHandler{&msgHandler{}}

	ctx := context.Background()
	mockRepo := mock_repository.NewMockRepository(ctrl)

	mockRepo.
		EXPECT().
		GetLastMessageInStream(ctx, "some id+position").
		Return(&repository.MessageEnvelope{}, nil)

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

	mySubscriber, err := myMessageStore.CreateSubscriber(
		"some id",
		handlers,
		SubscribeToEntityStream("some category", "1234"),
		SubscribeBatchSize(1),
	)

	if err != nil {
		t.Errorf("Failed on CreateSubscriber() Got: %s\n", err)
		return
	}

	pos, err := mySubscriber.GetPosition(ctx)

	if err != nil {
		t.Errorf("Failed on GetPosition() because of %v", err)
	}

	if pos != expectedPos {
		t.Errorf("Failed on GetPosition()\n Expected%d\n     Got: %d", expectedPos, pos)
	}
}
