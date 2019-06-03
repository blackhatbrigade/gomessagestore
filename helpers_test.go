package gomessagestore

import (
	"io/ioutil"

	"github.com/sirupsen/logrus"
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
