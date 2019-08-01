package gomessagestore

import (
	"context"
	"database/sql"

	"github.com/blackhatbrigade/gomessagestore/inmem_repository"
	"github.com/blackhatbrigade/gomessagestore/repository"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageStore > mocks/messagestore.go"

// MessageStore establishes the interface for Eventide
type MessageStore interface {
	Write(ctx context.Context, message Message, opts ...WriteOption) error                                         // writes a message to the message store
	Get(ctx context.Context, opts ...GetOption) ([]Message, error)                                                 // retrieves messages from the message store
	CreateProjector(opts ...ProjectorOption) (Projector, error)                                                    // creates a new projector
	CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) // creates a new subscriber
}

type msgStore struct {
	repo repository.Repository
}

// NewMessageStore creates a new MessageStore instance using an injected DB.
func NewMessageStore(injectedDB *sql.DB) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB)

	msgstr := &msgStore{
		repo: pgRepo,
	}

	return msgstr
}

// NewMessageStoreFromRepository creates a new MessageStore instance using an injected repository.
// FOR TESTING ONLY
func NewMessageStoreFromRepository(injectedRepo repository.Repository) MessageStore {
	msgstr := &msgStore{
		repo: injectedRepo,
	}

	return msgstr
}

// NewMockMessageStoreWithMessages is used for testing purposes
func NewMockMessageStoreWithMessages(msgs []Message) MessageStore {
	msgEnvs := make([]repository.MessageEnvelope, len(msgs))

	for i, msg := range msgs {
		msgEnv, _ := msg.ToEnvelope()
		msgEnvs[i] = *msgEnv
	}

	r := inmem_repository.NewInMemoryRepository(msgEnvs)
	return NewMessageStoreFromRepository(r)
}
