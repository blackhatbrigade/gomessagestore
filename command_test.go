package gomessagestore_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
)

func getSampleCommandMissing(key string) Command {
	cmd := getSampleCommand()

	switch key {
	case "MessageType":
		cmd.MessageType = ""
	case "StreamCategory":
		cmd.StreamCategory = ""
	case "ID":
		cmd.ID = NilUUID
	case "Data":
		cmd.Data = nil
	case "EntityID":
		cmd.EntityID = NilUUID
	}

	return cmd
}

func TestCommandToEnvelopeReturnsMessageEnvelope(t *testing.T) {
	cmd := getSampleCommand()

	msgEnv, _ := cmd.ToEnvelope()

	if msgEnv == nil {
		t.Error("Did not get a valid MessageEnvelope back from ToEnvelope")
	}
}

func TestCommandToEnvelopeErrorsIfNoType(t *testing.T) {
	cmd := getSampleCommand()

	cmd.MessageType = ""

	_, err := cmd.ToEnvelope()

	if err != ErrMissingMessageType {
		t.Error("Expected ErrMissingMessageType from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfNoCategory(t *testing.T) {
	cmd := getSampleCommand()

	cmd.StreamCategory = ""

	_, err := cmd.ToEnvelope()

	if err != ErrMissingMessageCategory {
		t.Error("Expected ErrMissingMessageCategory from ToEnvelope Call")
	}
}

//func TestCommandToEnvelopeErrorsIfCategoryContainsAHyphen(t *testing.T) {
//	cmd := getSampleCommand()
//
//	cmd.StreamCategory = "something-bad"
//
//	_, err := cmd.ToEnvelope()
//
//	if err != ErrInvalidMessageCategory {
//		t.Error("Expected ErrInvalidMessageCategory from ToEnvelope Call")
//	}
//}

func TestCommandToEnvelopeErrorsIfNoIDPresent(t *testing.T) {
	cmd := getSampleCommand()

	cmd.ID = NilUUID

	_, err := cmd.ToEnvelope()

	if err != ErrMessageNoID {
		t.Error("Expected ErrMessageNoID error from ToEnvelope Call")
	}
}

//TestCommandToEnvelope tests command.ToEnvelope
func TestCommandToEnvelope(t *testing.T) {
	data := []byte(`{"Field1":"a"}`)
	metadata := []byte(`{"Field1":"b"}`)
	categoryWithID := fmt.Sprintf("%s-%s", "test cat", uuid10)
	cmd1 := NewCommand(
		uuid1, uuid10, categoryWithID, "test type", data, metadata,
	)
	cmd1.MessageVersion = 10
	cmd1.GlobalPosition = 8
	cmd1.Time = time.Unix(1, 0)

	tests := []struct {
		name             string
		inputCommand     Command
		expectedEnvelope *repository.MessageEnvelope
		expectedError    error
		failEnvMessage   string
		failErrMessage   string
	}{{
		name:             "Returns message envelope",
		inputCommand:     cmd1,
		failEnvMessage:   "Did not get a valid MessageEnvelope back from ToEnvelope",
		expectedEnvelope: getSampleCommandAsEnvelopeWithID(),
	}, {
		name:           "Errors if no MessageType",
		inputCommand:   getSampleCommandMissing("MessageType"),
		expectedError:  ErrMissingMessageType,
		failErrMessage: "Expected ErrMissingMessageType from ToEnvelope Call",
	}, {
		name:           "Errors if Data is empty",
		inputCommand:   getSampleCommandMissing("Data"),
		expectedError:  ErrMissingMessageData,
		failErrMessage: "Expected ErrMissingMessageData from ToEnvelope",
	}, {
		name:           "Errors if no ID is present",
		inputCommand:   getSampleCommandMissing("ID"),
		expectedError:  ErrMessageNoID,
		failErrMessage: "Expected ErrMessageNoID from ToEnvelope",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgEnv, err := test.inputCommand.ToEnvelope()

			if err != test.expectedError {
				t.Errorf("Err: %s\nExpectedErr: %v\nActualErr: %v\n", test.failErrMessage, test.expectedError, err)
			}

			if !reflect.DeepEqual(msgEnv, test.expectedEnvelope) {
				t.Errorf("Err: %s\nExpectedEnvelope: %v\nActualEnvelope: %v\n", test.failEnvMessage, test.expectedEnvelope, msgEnv)
			}
		})
	}
}
