package gomessagestore_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/stretchr/testify/assert"
)

type slightlyComplicated struct {
	SomeCOMValue dummyData
	Value        *string `json:"somethingElse"`
}

func unpackHelper(source map[string]interface{}, dest *interface{}) (err error) {
	destValue := *dest
	switch data := destValue.(type) {
	case *dummyData:
		err = Unpack(source, &data)
	case *slightlyComplicated:
		err = Unpack(source, &data)
	default:
		err = errors.New(fmt.Sprintf("These aren't the data you're looking for: %s", reflect.TypeOf(destValue)))
	}

	return
}

func TestPack(t *testing.T) {
	testValue := "a string"
	tests := []struct {
		name            string
		expectedPackErr error
		input           interface{}
		expectedOutput  map[string]interface{}
	}{{
		name:            "doesn't work when given a string",
		input:           "some string",
		expectedPackErr: &json.UnmarshalTypeError{},
	}, {
		name:            "doesn't work when given a number",
		input:           5,
		expectedPackErr: &json.UnmarshalTypeError{},
	}, {
		name: "works when given nil",
	}, {
		name:  "works when given a small struct",
		input: &dummyData{"a"},
		expectedOutput: map[string]interface{}{
			"someField": "a",
		},
	}, {
		name:  "works when given a complicated struct",
		input: &slightlyComplicated{dummyData{"a"}, &testValue},
		expectedOutput: map[string]interface{}{
			"someComValue": map[string]interface{}{
				"someField": "a",
			},
			"somethingElse": "a string",
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			output, err := Pack(test.input)

			if test.expectedPackErr == nil {
				assert.Nil(err, "Err should be nil")
			} else if reflect.TypeOf(err) == reflect.TypeOf(errors.New("")) {
				assert.Equal(test.expectedPackErr, err, "Err should be our type of err")
				return
			} else {
				// for the json errors, rather than trying to match the exact error and message we match the type of error
				assert.IsType(test.expectedPackErr, err, "Err should be an off-the-shelf type of err")
				return
			}

			assert.Equal(test.expectedOutput, output, "Should be the same object at the end")
		})
	}
}

func TestUnpack(t *testing.T) {
	testValue := "a string"
	tests := []struct {
		name              string
		expectedPackErr   error
		expectedOutput    interface{}
		outputPlaceholder interface{}
		input             map[string]interface{}
	}{{
		name: "works when given nil",
	}, {
		name:              "works when given a small struct",
		expectedOutput:    &dummyData{"a"},
		outputPlaceholder: &dummyData{},
		input: map[string]interface{}{
			"someField": "a",
		},
	}, {
		name:              "works when given a complicated struct",
		expectedOutput:    &slightlyComplicated{dummyData{"a"}, &testValue},
		outputPlaceholder: &slightlyComplicated{},
		input: map[string]interface{}{
			"someComValue": map[string]interface{}{
				"someField": "a",
			},
			"somethingElse": "a string",
		},
	}, {
		name:              "works when given a complicated struct, with snake_case",
		expectedOutput:    &slightlyComplicated{dummyData{"a"}, &testValue},
		outputPlaceholder: &slightlyComplicated{},
		input: map[string]interface{}{
			"some_com_value": map[string]interface{}{
				"some_field": "a",
			},
			"something_else": "a string",
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			err := Unpack(test.input, &test.outputPlaceholder)

			if test.expectedPackErr == nil {
				assert.Nil(err, "Err should be nil")
			} else if reflect.TypeOf(err) == reflect.TypeOf(errors.New("")) {
				assert.Equal(test.expectedPackErr, err, "Err should be our type of err")
				return
			} else {
				// for the json errors, rather than trying to match the exact error and message we match the type of error
				assert.IsType(test.expectedPackErr, err, "Err should be an off-the-shelf type of err")
				return
			}

			assert.Equal(test.expectedOutput, test.outputPlaceholder, "Should be the same object at the end")
		})
	}
}

func TestPackUnpack(t *testing.T) {
	testValue := "a string"
	tests := []struct {
		name              string
		expectedPackErr   error
		expectedUnpackErr error
		input             interface{}
		expectedOutput    interface{}
		unpackCallBack    func(source map[string]interface{}, dest *interface{}) error
	}{{
		name:            "doesn't work when given a string",
		input:           "some string",
		expectedOutput:  "some string",
		expectedPackErr: &json.UnmarshalTypeError{},
	}, {
		name:            "doesn't work when given a number",
		input:           5,
		expectedOutput:  5,
		expectedPackErr: &json.UnmarshalTypeError{},
	}, {
		name:           "works when given nil",
		input:          nil,
		expectedOutput: nil,
	}, {
		name:           "works when given a small struct",
		input:          &dummyData{"a"},
		expectedOutput: &dummyData{"a"},
		unpackCallBack: unpackHelper,
	}, {
		name:           "works when given a complicated struct",
		input:          &slightlyComplicated{dummyData{"a"}, &testValue},
		expectedOutput: &slightlyComplicated{dummyData{"a"}, &testValue},
		unpackCallBack: unpackHelper,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			output, err := Pack(test.input)

			if test.expectedPackErr == nil {
				assert.Nil(err, "Err should be nil")
			} else if reflect.TypeOf(err) == reflect.TypeOf(errors.New("")) {
				assert.Equal(test.expectedPackErr, err, "Err should be our type of err")
				return
			} else {
				// for the json errors, rather than trying to match the exact error and message we match the type of error
				assert.IsType(test.expectedPackErr, err, "Err should be an off-the-shelf type of err")
				return
			}

			if test.unpackCallBack != nil {
				err = test.unpackCallBack(output, &test.input)
			} else {
				err = Unpack(output, &test.input) // reuse test.input so we have the right type
			}

			if test.expectedUnpackErr == nil {
				assert.Nil(err, "Err should be nil")
			} else if reflect.TypeOf(err) == reflect.TypeOf(errors.New("")) {
				assert.Equal(test.expectedUnpackErr, err, "Err should be our type of err")
				return
			} else {
				// for the json errors, rather than trying to match the exact error and message we match the type of error
				assert.IsType(test.expectedUnpackErr, err, "Err should be an off-the-shelf type of err")
				return
			}

			assert.Equal(test.expectedOutput, test.input, "Should be the same object at the end")
		})
	}
}

func TestMsgEnvelopesToMessages(t *testing.T) {
	tests := []struct {
		name           string
		input          []*repository.MessageEnvelope
		expectedOutput []Message
	}{{
		name:           "converts message envelopes to events",
		input:          getSampleEventsAsEnvelopes(),
		expectedOutput: eventsToMessageSlice(getSampleEvents()),
	}, {
		name:           "converts message envelopes to commands",
		input:          getSampleCommandsAsEnvelopes(),
		expectedOutput: commandsToMessageSlice(getSampleCommands()),
	}, {
		name:           "converts message envelopes with snake_case to commands with camelCase",
		input:          getSampleSnakeCaseCommandsAsEnvelopes(),
		expectedOutput: commandsToMessageSlice(getSampleCommands()),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			output := MsgEnvelopesToMessages(test.input)

			assert.Equal(test.expectedOutput, output)
		})
	}
}
