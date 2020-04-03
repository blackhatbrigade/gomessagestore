package repository

import (
	"context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	// writes
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
	WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
	// reads from stream
	GetAllMessagesInStream(ctx context.Context, streamName string, batchSize int) ([]*MessageEnvelope, error)
	GetAllMessagesInStreamSince(ctx context.Context, streamName string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error)
	GetLastMessageInStream(ctx context.Context, streamName string) (*MessageEnvelope, error)
	// reads from category
	GetAllMessagesInCategory(ctx context.Context, category string, batchSize int) ([]*MessageEnvelope, error)
	GetAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error)
}
