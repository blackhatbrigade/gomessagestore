package gomessagestore

import (
	"encoding/json"
)

//Unpack unpacks JSON-esque objects used in the Command and Event objects into GO objects
func Unpack(source map[string]interface{}, dest interface{}) error {
	inbetween, err := json.Marshal(source)
	if err != nil {
		return err
	}

	return json.Unmarshal(inbetween, dest)
}

//Pack packs a GO object into JSON-esque objects used in the Command and Event objects
func Pack(source interface{}) (map[string]interface{}, error) {
	dest := make(map[string]interface{})
	inbetween, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(inbetween, &dest)
	return dest, err
}
