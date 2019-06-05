package gomessagestore

import (
	"context"
	"database/sql"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageStore > mocks/messagestore.go"

//MessageStore Establishes the interface for Eventide.
type MessageStore interface {
	Write(ctx context.Context, message Message) error
	//WriteWithExpectedPosition(ctx context.Context, message *Message, version int64) error
}

type msgStore struct {
	repo repository.Repository
}

//GetMessageStoreInterface Grabs a MessageStore instance.
func GetMessageStoreInterface(injectedDB *sql.DB) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB)

	msgstr := &msgStore{
		repo: pgRepo,
	}

	return msgstr
}

//GetMessageStoreInterface2 Grabs a MessageStore instance.
func GetMessageStoreInterface2(injectedRepo repository.Repository) MessageStore {
	msgstr := &msgStore{
		repo: injectedRepo,
	}

	return msgstr
}

//Write Writes a Message to the message store.
func (ms *msgStore) Write(ctx context.Context, message Message) error {
	envelope, err := message.ToEnvelope()
	if err != nil {
		logrus.WithError(err).Error("Write: Validation Error")

		return err
	}

	err = ms.repo.WriteMessage(ctx, envelope)
	if err != nil {
		logrus.WithError(err).Error("Write: Error writing message")

		return err
	}

	return nil
}
