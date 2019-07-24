package gomessagestore

import "github.com/blackhatbrigade/gomessagestore/repository"

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Message > mocks/message.go"

//Message is an interface for all types of messages (commands, events, etc) that are handled through a message store.
type Message interface {
	ToEnvelope() (*repository.MessageEnvelope, error) // used to convert a message to a message envelope
	Type() string                                     // returns the message type
	Version() int64                                   // returns the version of the message
	Position() int64                                  // returns the position of the message
}
