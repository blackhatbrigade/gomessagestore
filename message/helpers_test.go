package message_test

import (
	"io/ioutil"

	"github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/sirupsen/logrus"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

// prevent weirdness with pointers
func CopyAndAppend(i []*message.MessageEnvelope, vals ...*message.MessageEnvelope) []*message.MessageEnvelope {
	j := make([]*message.MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

func getSampleCommand() *message.Command {
	packed, err := gomessagestore.Pack(dummyData{"a"})
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
	packed, err := gomessagestore.Pack(dummyData{"a"})
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
