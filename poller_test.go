package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestPoller(t *testing.T) {

	tests := []struct {
		name          string
		expectedError error
		subOpts       []SubscriberOption
		handlers      []MessageHandler
	}{{
		name:     "It ran",
		subOpts:  []SubscriberOption{},
		handlers: []MessageHandler{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			opts, err := GetSubscriberConfig(test.subOpts...)

			myWorker, err := CreateWorker(
				myMessageStore,
				"some id",
				test.handlers,
				opts,
			)
			if err != nil {
				t.Errorf("Failed on CreateWorker() Got: %s\n", err)
				return
			}

			myPoller, err := CreatePoller(
				myMessageStore,
				myWorker,
				opts,
			)
			if err != nil {
				t.Errorf("Failed on CreatePoller() Got: %s\n", err)
				return
			}

			err = myPoller.Poll(ctx)
			if err != nil {
				t.Errorf("Failed on Poll() Got: %s\n", err)
				return
			}
		})
	}
}
