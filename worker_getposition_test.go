package gomessagestore_test

import (
	"context"
	"testing"
	"time"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/golang/mock/gomock"
)

func TestSubscriberGetsPosition(t *testing.T) {

	tests := []struct {
		name             string
		subscriberID     string
		expectedError    error
		handlers         []MessageHandler
		expectedPosition int64
		expectedStream   string
		expectedCategory string
		opts             []SubscriberOption
		messages         []Message
		expectedHandled  []string
		positionEnvelope *repository.MessageEnvelope
	}{{
		name:             "When GetPosition is called (when no committed position exists) subscriber returns a position that matches the expected position",
		expectedPosition: 0,
		handlers:         []MessageHandler{&msgHandler{}},
		subscriberID:     "some id",
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "1234"),
			SubscribeBatchSize(1),
		},
	}, {
		name:             "When GetPosition is called (for category) subscriber returns a position that matches the expected position",
		expectedPosition: 400,
		handlers:         []MessageHandler{&msgHandler{}},
		subscriberID:     "some id",
		opts: []SubscriberOption{
			SubscribeToCategory("some category"),
			SubscribeBatchSize(1),
		},
		positionEnvelope: &repository.MessageEnvelope{
			ID:             uuid.NewRandom(),
			StreamName:     "I_am_subscriber_id+position",
			StreamCategory: "I_am_subscriber_id+position",
			MessageType:    "PositionCommitted",
			Version:        5,
			GlobalPosition: 500,
			Data:           []byte("{\"position\":400}"),
			Time:           time.Unix(1, 5),
		},
	}, {
		name:             "When GetPosition is called (for event stream) subscriber returns a position that matches the expected position",
		expectedPosition: 400,
		handlers:         []MessageHandler{&msgHandler{}},
		subscriberID:     "some id",
		opts: []SubscriberOption{
			SubscribeToEntityStream("some category", "1234"),
			SubscribeBatchSize(1),
		},
		positionEnvelope: &repository.MessageEnvelope{
			ID:             uuid.NewRandom(),
			StreamName:     "I_am_subscriber_id+position",
			StreamCategory: "I_am_subscriber_id+position",
			MessageType:    "PositionCommitted",
			Version:        5,
			GlobalPosition: 500,
			Data:           []byte("{\"position\":400}"),
			Time:           time.Unix(1, 5),
		},
	}, {
		name:             "When GetPosition is called (for command stream) subscriber returns a position that matches the expected position",
		expectedPosition: 400,
		handlers:         []MessageHandler{&msgHandler{}},
		subscriberID:     "some id",
		opts: []SubscriberOption{
			SubscribeToCommandStream("some category"),
			SubscribeBatchSize(1),
		},
		positionEnvelope: &repository.MessageEnvelope{
			ID:             uuid.NewRandom(),
			StreamName:     "I_am_subscriber_id+position",
			StreamCategory: "I_am_subscriber_id+position",
			MessageType:    "PositionCommitted",
			Version:        5,
			GlobalPosition: 500,
			Data:           []byte("{\"position\":400}"),
			Time:           time.Unix(1, 5),
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
				GetLastMessageInStream(ctx, "some id+position").
				Return(test.positionEnvelope, nil)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			opts, err := GetSubscriberConfig(test.opts...)
			panicIf(err)

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

			pos, err := myWorker.GetPosition(ctx)

			if err != nil {
				t.Errorf("Failed on GetPosition() because of %v", err)
			}

			if pos != test.expectedPosition {
				t.Errorf("Failed on GetPosition()\n Expected%d\n Got: %d", test.expectedPosition, pos)
			}
		})
	}
}
