package gomessagestore

import "github.com/blackhatbrigade/gomessagestore/repository"

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Message > mocks/message.go"

//Message Defines an interface that can define more specific class of messages such as Commands or Events.
type Message interface {
	ToEnvelope() (*repository.MessageEnvelope, error)
	Type() string
	Version() int64
	Position() int64
}
