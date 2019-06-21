package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestSubscriberStartCallsPoll(t *testing.T) {
	tests := []struct {
		name          string
		handlers      []MessageHandler
		expectedError error
		messages      []Message
		opts          []SubscriberOption
	}{{
		name:     "Start should cancel when given a context.Cancel()",
		handlers: []MessageHandler{&msgHandler{}},
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			mySubscriber, err := myMessageStore.CreateSubscriber(
				"someid",
				test.handlers,
				test.opts...,
			)
			if err != nil {
				t.Errorf("Failed on CreateSubscriber() Got: %s\n", err)
				return
			}

			err = mySubscriber.Start(ctx)
			if err != test.expectedError {
				t.Errorf("Failed to get expected error from ProcessMessages()\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}
		})
	}
}

/*
should be able to cancel (either via context.Cancel() or via a new Stop() function on subscriber)
		should not error when Poll() errors
		should wait defined intervals between calling Poll()
*/
