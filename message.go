package gomessagestore

import "github.com/blackhatbrigade/gomessagestore/repository"

//Message Defines an interface that can consume Commands or Events.
type Message interface {
	ToEnvelope() (*repository.MessageEnvelope, error)
}
