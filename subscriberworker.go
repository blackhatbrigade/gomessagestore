package gomessagestore

import (
	"context"
)

type SubscriptionWorker interface {
	GetMessages(ctx context.Context, position int64) ([]Message, error)
	ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error)
	GetPosition(ctx context.Context) (int64, error)
	SetPosition(ctx context.Context) error
}

type subscriptionWorker struct {
	opts     []SubscriberOption
	ms       MessageStore
	sub      *subscriber
	handlers []MessageHandler
}

func CreateWorker(ms MessageStore, handlers []MessageHandler, opts ...SubscriberOption) (*subscriptionWorker, error) {
	return &subscriptionWorker{
		ms:       ms,
		handlers: handlers,
		opts:     opts,
	}, nil
}
