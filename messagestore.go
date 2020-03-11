package gomessagestore

import (
	"context"
	"database/sql"

	"github.com/blackhatbrigade/gomessagestore/inmem_repository"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

// legend for messages
// * = application levev
// *ms = messagestore connector level
// ? = should it go anywhere?

// Go message store Logger
//{
// * "level": 30,
// * "time": 1583434429270,
// * "pid": 15909,
// * "hostname": "UTMIDNBELLMB",
// * "name": "@berkadia/example-digitization-component",
// *ms "connectorVersion": "1.0.0", <- go message store connection version
// * "traceId": "165060e3-5fa3-48c5-b46f-e507d35b21bf",  <- this is currently correaltionId
// ? "whereDisAt": "messageStoreConnector.write.writeMessageData",
// ? "tag": "profile",
// *ms "startTimeMs": 1583434429263,
// *ms "endTimeMs": 1583434429270,
// *ms "elapsedTimeMs": 7,
// * "v": 1
//}

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
	log  logrus.Logger
}

// NewMessageStore creates a new MessageStore instance using an injected DB.
func NewMessageStore(injectedDB *sql.DB, log logrus.Logger) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB)
	msgstr := &msgStore{
		repo: pgRepo,
		log:  log,
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
