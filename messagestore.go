package gomessagestore

import (
	"context"
	"database/sql"

	"github.com/blackhatbrigade/gomessagestore/inmem_repository"
	"github.com/blackhatbrigade/gomessagestore/repository"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageStore > mocks/messagestore.go"

//MessageStore Establishes the interface for Eventide.
type MessageStore interface {
	Write(ctx context.Context, message Message, opts ...WriteOption) error
	Get(ctx context.Context, opts ...GetOption) ([]Message, error)
	CreateProjector(opts ...ProjectorOption) (Projector, error)
	CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error)
}

type msgStore struct {
	repo repository.Repository
}

//NewMessageStore Grabs a MessageStore instance.
func NewMessageStore(injectedDB *sql.DB) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB)

	msgstr := &msgStore{
		repo: pgRepo,
	}

	return msgstr
}

//NewMessageStoreFromRepository Grabs a MessageStore instance.
func NewMessageStoreFromRepository(injectedRepo repository.Repository) MessageStore {
	msgstr := &msgStore{
		repo: injectedRepo,
	}

	return msgstr
}

//NewMockMessageStoreWithMessages
func NewMockMessageStoreWithMessages(msgs []Message) MessageStore {
	msgEnvs := make([]repository.MessageEnvelope, len(msgs))

	for i, msg := range msgs {
		msgEnv, _ := msg.ToEnvelope()
		msgEnvs[i] = *msgEnv
	}

	r := inmem_repository.NewInMemoryRepository(msgEnvs)
	return NewMessageStoreFromRepository(r)
}
