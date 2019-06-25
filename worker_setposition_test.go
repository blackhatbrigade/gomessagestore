package gomessagestore_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
)

func TestSetPosition(t *testing.T) {

	tests := []struct {
		name             string
		subscriberID     string
		expectedError    error
		handlers         []MessageHandler
		opts             []SubscriberOption
		message          Message
		positionEnvelope *repository.MessageEnvelope
	}{{
		name:         "When subscribed to a category it sets the position using global position",
		subscriberID: "someID",
		handlers:     []MessageHandler{&msgHandler{}},
		opts: []SubscriberOption{
			SubscribeToCategory("some cat"),
		},
		message: &Command{
			GlobalPosition: 3,
			MessageVersion: 2,
		},
		positionEnvelope: &repository.MessageEnvelope{
			StreamName:  "someID+position",
			MessageType: "PositionCommitted",
			Data:        []byte("{\"position\":3}"),
		},
	}, {
		name:         "When subscribed to a command stream it sets the position using version",
		subscriberID: "someID",
		handlers:     []MessageHandler{&msgHandler{}},
		opts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		message: &Command{
			GlobalPosition: 3,
			MessageVersion: 2,
		},
		positionEnvelope: &repository.MessageEnvelope{
			StreamName:  "someID+position",
			MessageType: "PositionCommitted",
			Data:        []byte("{\"position\":2}"),
		},
	}, {
		name:         "When subscribed to an entity stream it sets the position using version",
		subscriberID: "someID",
		handlers:     []MessageHandler{&msgHandler{}},
		opts: []SubscriberOption{
			SubscribeToEntityStream("entity stream cat", "entityID"),
		},
		message: &Command{
			GlobalPosition: 3,
			MessageVersion: 2,
		},
		positionEnvelope: &repository.MessageEnvelope{
			StreamName:  "someID+position",
			MessageType: "PositionCommitted",
			Data:        []byte("{\"position\":2}"),
		},
	}, {
		name:          "When repository returns an error subscription worker returns the error",
		subscriberID:  "someID",
		handlers:      []MessageHandler{&msgHandler{}},
		expectedError: errors.New("threw an error"),
		opts: []SubscriberOption{
			SubscribeToEntityStream("entity stream cat", "entityID"),
		},
		message: &Command{
			GlobalPosition: 3,
			MessageVersion: 2,
		},
		positionEnvelope: &repository.MessageEnvelope{
			StreamName:  "someID+position",
			MessageType: "PositionCommitted",
			Data:        []byte("{\"position\":2}"),
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
				WriteMessage(ctx, &envelopeMatcher{test.positionEnvelope}).
				Return(test.expectedError)

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			opts, err := GetSubscriberConfig(test.opts...)
			panicIf(err)

			myWorker, err := CreateWorker(
				myMessageStore,
				test.subscriberID,
				test.handlers,
				opts,
			)

			if err != nil {
				t.Errorf("Failed on CreateWorker() Got: %s\n", err)
				return
			}

			err = myWorker.SetPosition(ctx, test.message)

			if err != test.expectedError {
				t.Errorf("Failed on SetPosition() because of %v\nExpected: %v", err, test.expectedError)
			}
		})
	}
}

type envelopeMatcher struct {
	messageEnv *repository.MessageEnvelope
}

func (envMatcher *envelopeMatcher) String() string {
	return fmt.Sprintf("%+v", envMatcher.messageEnv)
}

func (envMatcher *envelopeMatcher) Matches(param interface{}) bool {
	switch s := param.(type) {
	case *repository.MessageEnvelope:
		if s.ID == uuid.Nil {
			return false
		}
		if envMatcher.messageEnv.StreamName != s.StreamName {
			return false
		}
		if envMatcher.messageEnv.StreamCategory != s.StreamCategory {
			return false
		}
		if envMatcher.messageEnv.MessageType != s.MessageType {
			return false
		}
		if envMatcher.messageEnv.Version != s.Version {
			return false
		}
		if envMatcher.messageEnv.GlobalPosition != s.GlobalPosition {
			return false
		}
		if !reflect.DeepEqual(envMatcher.messageEnv.Data, s.Data) {
			return false
		}
		if !reflect.DeepEqual(envMatcher.messageEnv.Metadata, s.Metadata) {
			return false
		}
		return true
	default:
		return false
	}
}
