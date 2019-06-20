package gomessagestore_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

var potato = errors.New("I'm a potato")

func TestSubscriberGetsMessages(t *testing.T) {
	messageHandler := &msgHandler{}

	tests := []struct {
		name             string
		expectedError    error
		handlers         []MessageHandler
		expectedPosition int64
		expectedStream   string
		expectedCategory string
		opts             []SubscriberOption
		messageEnvelopes []*repository.MessageEnvelope
		repoReturnError  error
	}{{
		name:             "When subscriber is called with SubscribeToEntityStream() option, repository is called correctly",
		expectedStream:   "some category-some id1",
		handlers:         []MessageHandler{messageHandler},
		expectedPosition: 5,
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "some id1"),
		},
	}, {
		name:             "When subscriber is called with SubscribeToCategory() option, repository is called correctly",
		expectedCategory: "some category",
		handlers:         []MessageHandler{messageHandler},
		expectedPosition: 5,
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
	}, {
		name:            "repository errors are passed on down",
		repoReturnError: potato,
		expectedError:   potato,
		handlers:        []MessageHandler{messageHandler},
		expectedStream:  "some category:command",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
		},
	}, {
		name:             "repository errors are passed on down",
		repoReturnError:  potato,
		expectedError:    potato,
		handlers:         []MessageHandler{messageHandler},
		expectedCategory: "some category",
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
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
		})
	}
}
