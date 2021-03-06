package gomessagestore_test

import (
	"context"
	"io/ioutil"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
)

func TestSubscriberProcessesMessages(t *testing.T) {

	tests := []struct {
		name                  string
		expectedError         error
		handlers              []MessageHandler
		opts                  []SubscriberOption
		messages              []Message
		expectedHandled       []string
		expectedFinalPosition int64
		expectedNumHandled    int
	}{{
		name: "Subscriber processes a message in the registered handler with command stream",
		handlers: []MessageHandler{
			&msgHandler{class: "Command MessageType 1"},
			&msgHandler{class: "Command MessageType 2"},
		},
		expectedHandled: []string{
			"Command MessageType 1",
			"Command MessageType 2",
		},
		opts: []SubscriberOption{
			SubscribeToCommandStream("category"),
		},
		messages:              commandsToMessageSlice(getSampleCommands()),
		expectedFinalPosition: 2, // second message, from Version
		expectedNumHandled:    2, // both messages
	}, {
		name: "Subscriber processes a message in the registered handler with entity stream",
		handlers: []MessageHandler{
			&msgHandler{class: "Event MessageType 1"},
			&msgHandler{class: "Event MessageType 2"},
		},
		expectedHandled: []string{
			"Event MessageType 1",
			"Event MessageType 2",
		},
		opts: []SubscriberOption{
			SubscribeToEntityStream("category", uuid1),
		},
		messages:              eventsToMessageSlice(getSampleEvents()),
		expectedFinalPosition: 8, // second message, from Version
		expectedNumHandled:    2, // both messages
	}, {
		name: "Subscriber processes a message in the registered handler with category",
		handlers: []MessageHandler{
			&msgHandler{class: "Event MessageType 1"},
			&msgHandler{class: "Event MessageType 2"},
		},
		expectedHandled: []string{
			"Event MessageType 1",
			"Event MessageType 2",
		},
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
		messages:              eventsToMessageSlice(getSampleEvents()),
		expectedFinalPosition: 349, // second message, from Position
		expectedNumHandled:    2,   // both messages
	}, {
		name:          "Subscriber processes a message in the registered handler with category, unless it receives an error",
		expectedError: potato,

		handlers: []MessageHandler{
			&msgHandler{class: "Event MessageType 2"},
			&msgHandler{class: "Event MessageType 1", retErr: potato}, // 1 comes after 2 in getSampleEvents
		},
		expectedHandled: []string{
			"Event MessageType 2",
		},
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
		messages:              eventsToMessageSlice(getSampleEvents()),
		expectedFinalPosition: 345, //  message, from Position
		expectedNumHandled:    1,   // only one message
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockRepo := mock_repository.NewMockRepository(ctrl)

			var logrusLogger = logrus.New()
			logrusLogger.Out = ioutil.Discard
			myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

			defaultOptions := []SubscriberOption{SubscribeLogger(logrusLogger)}
			opts, err := GetSubscriberConfig(append(defaultOptions, test.opts...)...)
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

			numHandled, posLastHandled, err := myWorker.ProcessMessages(ctx, test.messages)
			if err != test.expectedError {
				t.Errorf("Failed to get expected error from ProcessMessages()\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}

			if numHandled != test.expectedNumHandled {
				t.Errorf("Failed to get expected number-of-messages-handled from ProcessMessages()\nExpected: %d\n and got: %d\n", test.expectedNumHandled, numHandled)
			}

			if posLastHandled != test.expectedFinalPosition {
				t.Errorf("Failed to get expected final-position from ProcessMessages()\nExpected: %d\n and got: %d\n", test.expectedFinalPosition, posLastHandled)
			}

			handled := make([]string, 0, len(test.expectedHandled))
			for _, handlerI := range test.handlers {
				handler := handlerI.(*msgHandler)
				if !handler.called {
					t.Error("Handler was not called")
				}
				handled = append(handled, handler.handled...) // cause variable names are hard
			}
			if !reflect.DeepEqual(handled, test.expectedHandled) {
				t.Errorf("Handler was called for the wrong messages, \nCalled: %s\nExpected: %s\n", handled, test.expectedHandled)
			}
		})
	}
}
