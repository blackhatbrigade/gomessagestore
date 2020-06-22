package uuid_test

import (
	"testing"

	. "github.com/blackhatbrigade/gomessagestore/uuid"
)

func TestCanMarshallAllCaps(t *testing.T) {
	uuid, _ := Parse("5fe1e4d9-d788-4bec-9b01-8be766127727")

	marshalledLowercase, err := uuid.MarshalJSON()
	if err != nil {
		t.Errorf("An error occurred %+v", err)
	}
	if string(marshalledLowercase) != `"5fe1e4d9-d788-4bec-9b01-8be766127727"` {
		t.Error("Marshall capitalized the letters when it wasn't asked to")
	}

	uuid.AllCaps = true
	marshalledUppercase, err := uuid.MarshalJSON()
	if err != nil {
		t.Errorf("An error occurred %+v", err)
	}
	if string(marshalledUppercase) != `"5FE1E4D9-D788-4BEC-9B01-8BE766127727"` {
		t.Error("Marshall did not capitalize the letters")
	}
}

func TestCanStringAllCaps(t *testing.T) {
	uuid, _ := Parse("5fe1e4d9-d788-4bec-9b01-8be766127727")

	stringedLowercase := uuid.String()
	if stringedLowercase != `5fe1e4d9-d788-4bec-9b01-8be766127727` {
		t.Error("String capitalized the letters when it wasn't asked to")
	}

	uuid.AllCaps = true
	stringedUppercase := uuid.String()
	if stringedUppercase != `5FE1E4D9-D788-4BEC-9B01-8BE766127727` {
		t.Error("String did not capitalize the letters")
	}
}

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
