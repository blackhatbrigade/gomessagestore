package gomessagestore_test

import (
	"io/ioutil"

	"github.com/sirupsen/logrus"
  . "github.com/blackhatbrigade/gomessagestore"
)

// prevent weirdness with pointers
func copyAndAppend(i []*MessageEnvelope, vals ...*MessageEnvelope) []*MessageEnvelope {
	j := make([]*MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

func getSampleCommand() *Command {
	return &Command{
		Type:       "test type",
		Category:   "test cat",
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
    OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
    CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       dummyData{"a", "b"},
	}
}

func getSampleEvent() *Event {
	return &Event{
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
    CategoryID: "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Category:   "test cat",
    CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
    OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		Data:       dummyData{"a", "b"},
	}
}

