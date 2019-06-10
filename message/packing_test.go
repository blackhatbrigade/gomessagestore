package message_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/stretchr/testify/assert"
)

type slightlyComplicated struct {
	Nested dummyData
	Value  *string
}

func unpackHelper(source map[string]interface{}, dest *interface{}) (err error) {
	destValue := *dest
	switch data := destValue.(type) {
	case *dummyData:
		err = message.Unpack(source, &data)
	case *slightlyComplicated:
		err = message.Unpack(source, &data)
	default:
		err = errors.New(fmt.Sprintf("These aren't the data you're looking for: %s", reflect.TypeOf(destValue)))
	}

	return
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

			output, err := message.Pack(test.input)

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
				err = message.Unpack(output, &test.input) // reuse test.input so we have the right type
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
