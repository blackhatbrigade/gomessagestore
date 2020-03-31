package gomessagestore_test

import (
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
)

func getSampleEventMissing(key string) Event {
	event := getSampleEvent()

	switch key {
	case "ID":
		event.ID = NilUUID
	case "MessageType":
		event.MessageType = ""
	case "EntityID":
		event.EntityID = NilUUID
	case "StreamCategory":
		event.StreamCategory = ""
	case "Data":
		event.Data = nil
	}

	return event
}

func getSampleEventMalformed(key string) Event {
	event := getSampleEvent()

	switch key {
	case "CategoryHyphen":
		event.StreamCategory = "something-bad"
	}

	return event
}

//TestEventToEnvelope tests event.ToEnvelope
func TestEventToEnvelope(t *testing.T) {
	tests := []struct {
		name             string
		inputEvent       Event
		expectedEnvelope *repository.MessageEnvelope
		expectedError    error
		failEnvMessage   string
		failErrMessage   string
	}{{
		name:             "Returns message envelope",
		inputEvent:       getSampleEvent(),
		failEnvMessage:   "Didn't render the MessageEnvelope correctly",
		expectedEnvelope: getSampleEventAsEnvelope(),
	}, {
		name:           "Errors if no ID",
		inputEvent:     getSampleEventMissing("ID"),
		expectedError:  ErrMessageNoID,
		failErrMessage: "Expected a NEW ID for Event",
	}, {
		name:           "Errors if no EntityID",
		inputEvent:     getSampleEventMissing("EntityID"),
		expectedError:  ErrMissingMessageCategoryID,
		failErrMessage: "Expected a NEW ID for Event",
	}, {
		name:           "Errors if a hyphen is present in the StreamCategory name",
		inputEvent:     getSampleEventMalformed("CategoryHyphen"),
		expectedError:  ErrInvalidMessageCategory,
		failErrMessage: "Hyphen not allowed in StreamCategory name",
	}, {
		name:           "Errors if the category is left blank",
		inputEvent:     getSampleEventMissing("StreamCategory"),
		expectedError:  ErrMissingMessageCategory,
		failErrMessage: "StreamCategory Name must not be blank",
	}, {
		name:           "Errors if data is nil",
		inputEvent:     getSampleEventMissing("Data"),
		expectedError:  ErrMissingMessageData,
		failErrMessage: "Data must not be nil",
	}, {
		name:           "Errors if MessageType is left blank",
		inputEvent:     getSampleEventMissing("MessageType"),
		expectedError:  ErrMissingMessageType,
		failErrMessage: "MessageType must not be empty",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgEnv, err := test.inputEvent.ToEnvelope()

			if err != test.expectedError {
				t.Errorf("Err: %s\nExpected: %v\nActual: %v\n", test.failErrMessage, test.expectedError, err)
			}

			if !reflect.DeepEqual(msgEnv, test.expectedEnvelope) {
				t.Errorf("Err: %s\nExpected: %v\nActual: %v\n", test.failEnvMessage, test.expectedEnvelope, msgEnv)
			}
		})
	}
}
