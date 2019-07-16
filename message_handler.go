package gomessagestore

import "context"

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageHandler > mocks/message_handler.go"

type MessageHandler interface {
	Type() string
	Process(ctx context.Context, msg Message) error
}
