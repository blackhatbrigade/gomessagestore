package message_test

import (
	"reflect"
	"testing"

	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/blackhatbrigade/gomessagestore/testutils"
)

func getSampleEventMissing(key string) *message.Event {
	event := testutils.GetSampleEvent()

	switch key {
	case "NewID":
		event.NewID = ""
	case "Type":
		event.Type = ""
	case "CategoryID":
		event.CategoryID = ""
	case "Category":
		event.Category = ""
	case "CausedByID":
		event.CausedByID = ""
	case "OwnerID":
		event.OwnerID = ""
	case "Data":
		event.Data = nil
	}

	return event
}

func getSampleEventMalformed(key string) *message.Event {
	event := testutils.GetSampleEvent()

	switch key {
	case "CategoryHyphen":
		event.Category = "something-bad"
	}

	return event
}

func getSampleCommandMissing(key string) *message.Command {
	cmd := testutils.GetSampleCommand()

	switch key {
	case "Type":
		cmd.Type = ""
	case "Category":
		cmd.Category = ""
	case "NewID":
		cmd.NewID = ""
	case "CausedByID":
		cmd.CausedByID = ""
	case "OwnerID":
		cmd.OwnerID = ""
	case "Data":
		cmd.Data = nil
	}

	return cmd
}

func getSampleCommandMalformed(key string) *message.Command {
	cmd := testutils.GetSampleCommand()

	switch key {
	case "CategoryHyphen":
		cmd.Category = "something-bad"
	}

	return cmd
}

func TestCommandToEnvelopeReturnsMessageEnvelope(t *testing.T) {
	cmd := testutils.GetSampleCommand()

	msgEnv, _ := cmd.ToEnvelope()

	if msgEnv == nil {
		t.Error("Did not get a valid MessageEnvelope back from ToEnvelope")
	}
}

func TestCommandToEnvelopeErrorsIfNoType(t *testing.T) {
	cmd := testutils.GetSampleCommand()

	cmd.Type = ""

	_, err := cmd.ToEnvelope()

	if err != message.ErrMissingMessageType {
		t.Error("Expected ErrMissingMessageType from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfNoCategory(t *testing.T) {
	cmd := testutils.GetSampleCommand()

	cmd.Category = ""

	_, err := cmd.ToEnvelope()

	if err != message.ErrMissingMessageCategory {
		t.Error("Expected ErrMissingMessageCategory from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfCategoryContainsAHyphen(t *testing.T) {
	cmd := testutils.GetSampleCommand()

	cmd.Category = "something-bad"

	_, err := cmd.ToEnvelope()

	if err != message.ErrInvalidMessageCategory {
		t.Error("Expected ErrInvalidMessageCategory from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfNoIDPresent(t *testing.T) {
	cmd := testutils.GetSampleCommand()

	cmd.NewID = ""

	_, err := cmd.ToEnvelope()

	if err != message.ErrMessageNoID {
		t.Error("Expected ErrMessageNoID error from ToEnvelope Call")
	}
}

//TestCommandToEnvelope tests command.ToEnvelope
func TestCommandToEnvelope(t *testing.T) {
	tests := []struct {
		name             string
		inputCommand     *message.Command
		expectedEnvelope *message.MessageEnvelope
		expectedError    error
		failEnvMessage   string
		failErrMessage   string
	}{{
		name:           "Returns message envelope",
		inputCommand:   testutils.GetSampleCommand(),
		failEnvMessage: "Did not get a valid MessageEnvelope back from ToEnvelope",
		expectedEnvelope: &message.MessageEnvelope{
			MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
			Type:       "test type",
			Stream:     "test cat:command",
			StreamType: "test cat",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			Data:       []byte(`{"Field1":"a"}`),
		},
	}, {
		name:           "Errors if no Type",
		inputCommand:   getSampleCommandMissing("Type"),
		expectedError:  message.ErrMissingMessageType,
		failErrMessage: "Expected ErrMissingMessageType from ToEnvelope Call",
	}, {
		name:           "Errors if Data is empty",
		inputCommand:   getSampleCommandMissing("Data"),
		expectedError:  message.ErrMissingMessageData,
		failErrMessage: "Expected ErrMissingMessageData from ToEnvelope",
	}, {
		name:           "Errors if no ID is present",
		inputCommand:   getSampleCommandMissing("NewID"),
		expectedError:  message.ErrMessageNoID,
		failErrMessage: "Expected ErrMessageNoID from ToEnvelope",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgEnv, err := test.inputCommand.ToEnvelope()

			if err != test.expectedError {
				t.Errorf("Err: %s\nExpected: %v\nActual: %v\n", test.failErrMessage, test.expectedError, err)
			}

			if !reflect.DeepEqual(msgEnv, test.expectedEnvelope) {
				t.Errorf("Err: %s\nExpected: %v\nActual: %v\n", test.failEnvMessage, test.expectedEnvelope, msgEnv)
			}
		})
	}
}

//TestEventToEnvelope tests event.ToEnvelope
func TestEventToEnvelope(t *testing.T) {
	tests := []struct {
		name             string
		inputEvent       *message.Event
		expectedEnvelope *message.MessageEnvelope
		expectedError    error
		failEnvMessage   string
		failErrMessage   string
	}{{
		name:           "Returns message envelope",
		inputEvent:     GetSampleEvent(),
		failEnvMessage: "Didn't render the MessageEnvelope correctly",
		expectedEnvelope: &message.MessageEnvelope{
			MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
			Type:       "test type",
			Stream:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamType: "test cat",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			Data:       []byte(`{"Field1":"a"}`),
		},
	}, {
		name:           "Errors if no NewID",
		inputEvent:     getSampleEventMissing("NewID"),
		expectedError:  message.ErrMessageNoID,
		failErrMessage: "Expected a NEW ID for Event",
	}, {
		name:           "Errors if no CategoryID",
		inputEvent:     getSampleEventMissing("CategoryID"),
		expectedError:  message.ErrMissingMessageCategoryID,
		failErrMessage: "Expected a NEW ID for Event",
	}, {
		name:           "Errors if a hyphen is present in the Category name",
		inputEvent:     getSampleEventMalformed("CategoryHyphen"),
		expectedError:  message.ErrInvalidMessageCategory,
		failErrMessage: "Hyphen not allowed in Category name",
	}, {
		name:           "Errors if the category is left blank",
		inputEvent:     getSampleEventMissing("Category"),
		expectedError:  message.ErrMissingMessageCategory,
		failErrMessage: "Category Name must not be blank",
	}, {
		name:           "Errors if data is nil",
		inputEvent:     getSampleEventMissing("Data"),
		expectedError:  message.ErrMissingMessageData,
		failErrMessage: "Data must not be nil",
	}, {
		name:           "Errors if Type is left blank",
		inputEvent:     getSampleEventMissing("Type"),
		expectedError:  message.ErrMissingMessageType,
		failErrMessage: "Type must not be empty",
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
