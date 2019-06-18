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

func TestSubscriberPoll(t *testing.T) {
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
		name:           "Repository is called for a stream when Poll method is invoked",
		subscriberID:   "some id",
		expectedStream: "some category-some id1",
		handlers:       []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "some id1"),
		},
	}, {
		name:             "Repository is called for a category when Poll method is invoked",
		subscriberID:     "some id",
		expectedCategory: "some category",
		handlers:         []MessageHandler{messageHandler},
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
		},
	}, {
		name:           "Repository is called for a command stream  when Poll method is invoked",
		handlers:       []MessageHandler{messageHandler},
		expectedStream: "some category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
		},
	}, {
		name:                "Subscriber Poll processes a message in the registered handler",
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
			mockRepo.EXPECT().GetLastMessageInStream(ctx, "some id+position").Return(test.positionEnvelope, nil)

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

			err = mySubscriber.Poll(ctx)
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

func TestPollKeepsTrackOfPositionForStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handlers := []MessageHandler{&msgHandler{}}
	expectedStream := "some category-1234"

	ctx := context.Background()
	mockRepo := mock_repository.NewMockRepository(ctrl)

	messageEnvelopes := getSampleCommandsAsEnvelopes()

	firstCall := mockRepo.
		EXPECT().
		GetAllMessagesInStreamSince(ctx, expectedStream, 0).
		Return(messageEnvelopes, nil)

	mockRepo.
		EXPECT().
		GetAllMessagesInStreamSince(ctx, expectedStream, messageEnvelopes[1].Version+1).
		Return(messageEnvelopes, nil).
		After(firstCall)

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

	err = mySubscriber.Poll(ctx)

	if err != nil {
		t.Errorf("Failed first call: %v", err)
	}
	err = mySubscriber.Poll(ctx)
	if err != nil {
		t.Errorf("Failed second call: %v", err)
	}
}
