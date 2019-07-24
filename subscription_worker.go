package gomessagestore

import (
	"context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore SubscriptionWorker > mocks/subscriptionworker.go"

// SubscriptionWorker handles the processes for retrieving and processing messages from the message store and updating positions
type SubscriptionWorker interface {
	GetMessages(ctx context.Context, position int64) ([]Message, error)
	ProcessMessages(ctx context.Context, msgs []Message) (messagesHandled int, positionOfLastHandled int64, err error)
	GetPosition(ctx context.Context) (int64, error)
	SetPosition(ctx context.Context, position int64) error
}

type subscriptionWorker struct {
	config       *SubscriberConfig
	ms           MessageStore
	handlers     []MessageHandler
	subscriberID string
}

// CreateWorker returns a new subscriptionWorker
func CreateWorker(ms MessageStore, subscriberID string, handlers []MessageHandler, config *SubscriberConfig) (SubscriptionWorker, error) {
	return &subscriptionWorker{
		ms:           ms,
		handlers:     handlers,
		config:       config,
		subscriberID: subscriberID,
	}, nil
}
