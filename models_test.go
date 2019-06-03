package gomessagestore

import (
  "testing"
)

func getSampleCommand() *Command {
  return &Command{
    Type: "test type",
    Category: "test cat",
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
