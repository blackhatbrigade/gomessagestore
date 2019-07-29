package gomessagestore

import "context"

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageHandler > mocks/message_handler.go"

// MessageHandler is used to process messages; a handler should exist for each message type
type MessageHandler interface {
	Type() string                                   // returns the message type
	Process(ctx context.Context, msg Message) error // called for each message being handled
}
