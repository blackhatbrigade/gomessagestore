package uuid

import "testing"

func CanMarshallAndUnMarshall(t *testing.T) {
	uuid := NewRandom()
	marshalled, err := uuid.MarshalJSON()
	if err != nil {
		t.Errorf("An error occurred %+v", err)
	}
	err = uuid.UnmarshalJSON(marshalled)
	if err != nil {
		t.Error("Marshall and Unmarshall not working properly")
	}
}

func ParseAndString(t *testing.T) {
}
