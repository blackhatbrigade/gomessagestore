package gomessagestore

import (
	"testing"
)

func getSampleCommand() *Command {
	return &Command{
		Type:     "test type",
		Category: "test cat",
		NewID:    "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Data:     "DataDataData",
	}
}

func TestCommandToEnvelopeReturnsMessageEnvelope(t *testing.T) {
	cmd := getSampleCommand()

	me, _ := cmd.ToEnvelope()

	if me == nil {
		t.Error("Did not get a valid MessageEnvelope back from ToEnvelope")
	}
}

func TestCommandToEnvelopeErrorsIfNoType(t *testing.T) {
	cmd := getSampleCommand()

	cmd.Type = ""

	_, err := cmd.ToEnvelope()

	if err != ErrMissingMessageType {
		t.Error("Expected ErrMissingMessageType from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfNoCategory(t *testing.T) {
	cmd := getSampleCommand()

	cmd.Category = ""

	_, err := cmd.ToEnvelope()

	if err != ErrMissingMessageCategory {
		t.Error("Expected ErrMissingMessageCategory from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfCategoryContainsAHyphen(t *testing.T) {
	cmd := getSampleCommand()

	cmd.Category = "something-bad"

	_, err := cmd.ToEnvelope()

	if err != ErrInvalidMessageCategory {
		t.Error("Expected ErrInvalidMessageCategory from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfNoIDPresent(t *testing.T) {
	cmd := getSampleCommand()

	cmd.NewID = ""

	_, err := cmd.ToEnvelope()

	if err != ErrMessageNoID {
		t.Error("Expected ErrMessageNoID error from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfDataIsNil(t *testing.T) {
	cmd := getSampleCommand()

	cmd.Data = nil

	_, err := cmd.ToEnvelope()

	if err != ErrMissingMessageData {
		t.Error("Expected ErrMissingMessageData error from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfDataIsAPointerToNil(t *testing.T) {
	cmd := getSampleCommand()

	var nilPointer *int

	cmd.Data = nilPointer

	_, err := cmd.ToEnvelope()

	if err != ErrDataIsNilPointer {
		t.Error("Expected ErrDataIsNilPointer error from ToEnvelope Call")
	}
}

func TestCommandToEnvelopeErrorsIfDataCantBeMarshalledToJSON(t *testing.T) {
	cmd := getSampleCommand()

	cmd.Data = make(map[*string]int)

	_, err := cmd.ToEnvelope()

	if err != ErrUnserializableData {
		t.Error("Expected ErrUnserializableData error from ToEnvelope Call")
	}
}
