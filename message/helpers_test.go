package message_test

import (
	"io/ioutil"

	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/sirupsen/logrus"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

type dummyData struct {
	Field1 string // more than 1 field here breaks idempotency of tests because of json marshalling from a map[string]interface{} type
}

func getSampleCommand() *message.Command {
	packed, err := message.Pack(dummyData{"a"})
	panicIf(err)
	return &message.Command{
		Type:       "test type",
		Category:   "test cat",
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       packed,
	}
}

func getSampleEvent() *message.Event {
	packed, err := message.Pack(dummyData{"a"})
	panicIf(err)
	return &message.Event{
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		CategoryID: "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Category:   "test cat",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		Data:       packed,
	}
}
