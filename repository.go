package gomessagestore

import (
	"golang.org/x/net/context"
)

//go:generate bash -c "mockgen dataIQ-application-redIQ/src/messagestore Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	FindAllMessagesSince(ctx context.Context, position int64) ([]*MessageEnvelope, error)
	FindAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error)
  FindLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error)
	FindSubscriberPosition(ctx context.Context, subscriberID string) (int64, error)
	SetSubscriberPosition(ctx context.Context, subscriberID string, position int64) error
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
  WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
}

