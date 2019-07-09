package gomessagestore

import "context"

type MessageHandler interface {
	Type() string
	Process(ctx context.Context, ms MessageStore, msg Message) error
}
