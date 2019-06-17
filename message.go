package gomessagestore

import "github.com/blackhatbrigade/gomessagestore/repository"

//Message Defines an interface that can define more specific class of messages such as Commands or Events.
type Message interface {
	ToEnvelope() (*repository.MessageEnvelope, error)
	Type() string
}
