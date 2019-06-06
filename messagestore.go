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
	Write(ctx context.Context, message Message, opts ...WriteOption) error
	Get(ctx context.Context, opts ...GetOption) ([]Message, error)
	//WriteWithExpectedPosition(ctx context.Context, message *Message, version int64) error
}

type msgStore struct {
	repo repository.Repository
}

type writer struct {
	atPosition *int64
}

type getter struct{}

//WriteOption provide optional arguments to the Write function
type WriteOption func(w *writer)

//GetOption provide optional arguments to the Get function
type GetOption func(g *getter)

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

func checkWriteOptions(opts ...WriteOption) *writer {
	w := &writer{}
	for _, option := range opts {
		option(w)
	}
	return w
}

//Write Writes a Message to the message store.
func (ms *msgStore) Write(ctx context.Context, message Message, opts ...WriteOption) error {
	envelope, err := message.ToEnvelope()
	if err != nil {
		logrus.WithError(err).Error("Write: Validation Error")

		return err
	}

	writeOptions := checkWriteOptions(opts...)
	if writeOptions.atPosition != nil {
		err = ms.repo.WriteMessageWithExpectedPosition(ctx, envelope, *writeOptions.atPosition)
	} else {
		err = ms.repo.WriteMessage(ctx, envelope)
	}
	if err != nil {
		logrus.WithError(err).Error("Write: Error writing message")

		return err
	}
	return nil
}

//Get Gets one or more Messages from the message store.
func (ms *msgStore) Get(ctx context.Context, opts ...GetOption) ([]Message, error) {
	return nil, nil
}

//AtPosition allows for writing messages using an expected position
func AtPosition(position int64) WriteOption {
	return func(w *writer) {
		w.atPosition = &position
	}
}
