package uuid_test

import (
	"testing"

	. "github.com/blackhatbrigade/gomessagestore/uuid"
)

func TestCanMarshallAndUnMarshall(t *testing.T) {
	uuid := NewRandom()
	marshalled, err := uuid.MarshalJSON()
	if err != nil {
		t.Errorf("An error occurred %+v", err)
	}
	newUUID := NewRandom()
	err = newUUID.UnmarshalJSON(marshalled)
	if err != nil {
		t.Error("Failed to unmarshall")
	}
	if newUUID != uuid {
		t.Error("Marshall and Unmarshall not working properly")
	}
}

func TestParseAndString(t *testing.T) {
	uuid := NewRandom()

	stringedUUID := uuid.String()

	parsedUUID, err := Parse(stringedUUID)
	if err != nil {
		t.Errorf("Parse string to uuid failed because: %+v", err)
	}
	if parsedUUID != uuid {
		t.Error("FIRE")
	}
}
