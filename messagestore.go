package gomessagestore

import (
	"context"
	"database/sql"

	"github.com/blackhatbrigade/gomessagestore/inmem_repository"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

// legend for messages
// * = application level
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

	//a receive log as an option
	//b there is a function on CreateSub func that receives a log
	Write(ctx context.Context, message Message, opts ...WriteOption) error                                         // writes a message to the message store
	Get(ctx context.Context, opts ...GetOption) ([]Message, error)                                                 // retrieves messages from the message store
	CreateProjector(opts ...ProjectorOption) (Projector, error)                                                    // creates a new projector
	CreateSubscriber(subscriberID string, handlers []MessageHandler, opts ...SubscriberOption) (Subscriber, error) // creates a new subscriber
	getLogger() (logger logrus.FieldLogger)                                                                        // gets the logger
}

type msgStore struct {
	repo repository.Repository
	log  logrus.FieldLogger
}

// NewMessageStore creates a new MessageStore instance using an injected DB.
func NewMessageStore(injectedDB *sql.DB, logger logrus.FieldLogger) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB, logger)
	msgstr := &msgStore{
		repo: pgRepo,
		log:  logger,
	}

	return msgstr
}

// NewMessageStoreFromRepository creates a new MessageStore instance using an injected repository.
// FOR TESTING ONLY
func NewMessageStoreFromRepository(injectedRepo repository.Repository, logger logrus.FieldLogger) MessageStore {
	msgstr := &msgStore{
		repo: injectedRepo,
		log:  logger,
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
	return NewMessageStoreFromRepository(r, logrus.New()) // passing in a log from the outside doesn't make sense here as we're just doing testing
}

// This function gets the logger we need for other pieces
func (ms *msgStore) getLogger() logrus.FieldLogger {
	if ms.log == nil {
		var logger = logrus.New()
		return logger
	} else {
		return ms.log
	}
}
