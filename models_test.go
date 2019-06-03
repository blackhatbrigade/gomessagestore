package gomessagestore_test

import (
  "reflect"
	"testing"

  . "github.com/blackhatbrigade/gomessagestore"
)

type dummyData struct {
  Field1 string
  Field2 string
}

func getSampleEventMissing(key string) *Event {
  event := getSampleEvent()

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

func getSampleEventMalformed(key string) *Event {
  event := getSampleEvent()

  switch key {
  case "DataNil":
    var nilPointer *int
    event.Data = nilPointer
  case "CategoryHyphen":
    event.Category = "something-bad"
  }

  return event
}


func getSampleCommandMissing(key string) *Command {
  cmd := getSampleCommand()

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

func TestCommandToEnvelopeReturnsMessageEnvelope(t *testing.T) {
	cmd := getSampleCommand()

	msgEnv, _ := cmd.ToEnvelope()

	if msgEnv == nil {
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

func TestCommandToEnvelopeReturnsValidEnvelopeMapping(t *testing.T) {
  cmd := getSampleCommand()

  msgEnv, err := cmd.ToEnvelope()

  if err != nil {
    t.Error("Should not error, should create an envelope")
  }

  testEnvelope := &MessageEnvelope{
		MessageID:     "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:          "test type",
		Stream:        "test cat:command",
		StreamType:    "test cat",
    OwnerID:       "544477d6-453f-4b48-8460-0a6e4d6f97e5",
    CausedByID:    "544477d6-453f-4b48-8460-0a6e4d6f97d7",
    Data:          []byte(`{"Field1":"a","Field2":"b"}`),
	}

  if !reflect.DeepEqual(msgEnv, testEnvelope) {
    t.Error("Expected MessageEnvelope contents to match original Command contents")
  }
}

//TestEventToEnvelope tests event.ToEnvelope
func TestEventToEnvelope(t *testing.T) {
  tests := []struct {
    name string
    inputEvent *Event
    expectedEnvelope *MessageEnvelope
    expectedError error
    failEnvMessage string
    failErrMessage string
  }{{
    name: "Returns message envelope",
    inputEvent: getSampleEvent(),
    failEnvMessage: "Didn't render the MessageEnvelope correctly",
    expectedEnvelope: &MessageEnvelope{
      MessageID:     "544477d6-453f-4b48-8460-0a6e4d6f97d5",
      Type:          "test type",
      Stream:        "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
      StreamType:    "test cat",
      OwnerID:       "544477d6-453f-4b48-8460-0a6e4d6f97e5",
      CausedByID:    "544477d6-453f-4b48-8460-0a6e4d6f97d7",
      Data:          []byte(`{"Field1":"a","Field2":"b"}`),
    },
  }, {
    name: "Errors if no NewID",
    inputEvent: getSampleEventMissing("NewID"),
    expectedError: ErrMessageNoID,
    failErrMessage: "Expected a NEW ID for Event",
  }, {
    name: "Errors if no CategoryID",
    inputEvent: getSampleEventMissing("CategoryID"),
    expectedError: ErrMissingMessageCategoryID,
    failErrMessage: "Expected a NEW ID for Event",
  }, {
    name: "Errors if a hyphen is present in the Category name",
    inputEvent: getSampleEventMalformed("CategoryHyphen"),
    expectedError: ErrInvalidMessageCategory,
    failErrMessage: "Hyphen not allowed in Category name",
  }, {
    name: "Errors if the category is left blank",
    inputEvent: getSampleEventMissing("Category"),
    expectedError: ErrMissingMessageCategory,
    failErrMessage: "Category Name must not be blank",
  }, {
    name: "Errors if data is nil",
    inputEvent: getSampleEventMissing("Data"),
    expectedError: ErrMissingMessageData,
    failErrMessage: "Data must not be nil",
  }, {
    name: "Errors if Type is left blank",
    inputEvent: getSampleEventMissing("Type"),
    expectedError: ErrMissingMessageType,
    failErrMessage: "Type must not be empty",
  }, {
    name: "Errors if Data is given an empty pointer",
    inputEvent: getSampleEventMalformed("DataNil"),
    expectedError: ErrDataIsNilPointer,
    failErrMessage: "Can not provide an empty pointer",
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

//TestCommandToEnvelope tests command.ToEnvelope
func TestCommandToEnvelope(t *testing.T) {
  tests := []struct {
    name string
    inputCommand *Command
    expectedEnvelope *MessageEnvelope
    expectedError error
    failEnvMessage string
    failErrMessage string
  }{{
    name: "Returns message envelope",
    inputCommand: getSampleCommand(),
    failEnvMessage: "Did not get a valid MessageEnvelope back from ToEnvelope",
    expectedEnvelope: &MessageEnvelope{
      MessageID:     "544477d6-453f-4b48-8460-0a6e4d6f97d5",
      Type:          "test type",
      Stream:        "test cat:command",
      StreamType:    "test cat",
      OwnerID:       "544477d6-453f-4b48-8460-0a6e4d6f97e5",
      CausedByID:    "544477d6-453f-4b48-8460-0a6e4d6f97d7",
      Data:          []byte(`{"Field1":"a","Field2":"b"}`),
    },
  }, {
    name: "Errors if no Type",
    inputCommand: getSampleCommandMissing("Type"),
    expectedError: ErrMissingMessageType,
    failErrMessage: "Expected ErrMissingMessageType from ToEnvelope Call",
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
