package message

//Message Defines an interface that can consume Commands or Events.
type Message interface {
	ToEnvelope() (*MessageEnvelope, error)
}
