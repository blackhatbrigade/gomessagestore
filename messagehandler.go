package gomessagestore

import "context"

type MessageHandler interface {
	Process(ctx context.Context, msg Message) error
}
