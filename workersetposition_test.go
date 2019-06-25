package gomessagestore_test

import (
	"context"
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
		name:         "When subscribed to a command stream it sets the position using global position",
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
			Data:        []byte("{\"position\":3}"),
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
				Return(nil)

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

			if err != nil {
				t.Errorf("Failed on SetPosition() because of %v", err)
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
		if !IsValidUUID(s.ID) {
			fmt.Println("1")
			return false
		}
		if envMatcher.messageEnv.StreamName != s.StreamName {
			fmt.Println("2")
			return false
		}
		if envMatcher.messageEnv.StreamCategory != s.StreamCategory {
			fmt.Println("3")
			return false
		}
		if envMatcher.messageEnv.MessageType != s.MessageType {
			fmt.Println("4")
			return false
		}
		if envMatcher.messageEnv.Version != s.Version {
			fmt.Println("5")
			return false
		}
		if envMatcher.messageEnv.GlobalPosition != s.GlobalPosition {
			fmt.Println("6")
			return false
		}
		if !reflect.DeepEqual(envMatcher.messageEnv.Data, s.Data) {
			fmt.Println("7")
			return false
		}
		if !reflect.DeepEqual(envMatcher.messageEnv.Metadata, s.Metadata) {
			fmt.Println("8")
			return false
		}
		fmt.Println("9")
		return true
	default:
		fmt.Println("10")
		return false
	}
}

func IsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}
