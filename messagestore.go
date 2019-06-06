package gomessagestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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

type getter struct {
	stream *string
}

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

func checkGetOptions(opts ...GetOption) *getter {
	g := &getter{}
	for _, option := range opts {
		option(g)
	}
	return g
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

func (ms *msgStore) MsgEnvelopesToMessages(msgEnvelopes []*repository.MessageEnvelope) []Message {
	messages := make([]Message, 0, len(msgEnvelopes))
	for _, messageEnvelope := range msgEnvelopes {
		if messageEnvelope == nil {
			logrus.Error("Found a nil in the message envelope slice, can't transform to a message")
			continue
		}
		data := make(map[string]interface{})
		err := json.Unmarshal(messageEnvelope.Data, &data)
		if err != nil {
			logrus.WithError(err).Error("Can't unmarshal JSON from message envelope")
			continue
		}
		if strings.HasSuffix(messageEnvelope.Stream, ":command") {
			command := &Command{
				NewID:      messageEnvelope.MessageID,
				Type:       messageEnvelope.Type,
				Category:   strings.TrimSuffix(messageEnvelope.Stream, ":command"),
				CausedByID: messageEnvelope.CausedByID,
				OwnerID:    messageEnvelope.OwnerID,
				Data:       data,
			}
			messages = append(messages, command)
		} else {
			category, id := "", ""
			cats := strings.SplitN(messageEnvelope.Stream, "-", 2)
			if len(cats) > 0 {
				category = cats[0]
				if len(cats) == 2 {
					id = cats[1]
				}
			}
			event := &Event{
				NewID:      messageEnvelope.MessageID,
				Type:       messageEnvelope.Type,
				Category:   category,
				CategoryID: id,
				CausedByID: messageEnvelope.CausedByID,
				OwnerID:    messageEnvelope.OwnerID,
				Data:       data,
			}
			messages = append(messages, event)
		}
	}

	return messages
}

//Get Gets one or more Messages from the message store.
func (ms *msgStore) Get(ctx context.Context, opts ...GetOption) ([]Message, error) {

	if len(opts) == 0 {
		return nil, ErrMissingGetOptions
	}

	getOptions := checkGetOptions(opts...)
	msgEnvelopes, err := ms.repo.FindAllMessagesInStream(ctx, *getOptions.stream)

	if err != nil {
		logrus.WithError(err).Error("Get: Error getting message")

		return nil, err
	}
	return ms.MsgEnvelopesToMessages(msgEnvelopes), nil
}

//AtPosition allows for writing messages using an expected position
func AtPosition(position int64) WriteOption {
	return func(w *writer) {
		w.atPosition = &position
	}
}

//Stream allows for writing messages using an expected position
func CommandStream(stream string) GetOption {
	return func(g *getter) {
		stream := fmt.Sprintf("%s:command", stream)
		g.stream = &stream
	}
}

//Stream allows for writing messages using an expected position
func EventStream(category, entityID string) GetOption {
	return func(g *getter) {
		stream := fmt.Sprintf("%s-%s", category, entityID)
		g.stream = &stream
	}
}

//Unpack unpacks JSON-esque objects used in the Command and Event objects into GO objects
func Unpack(source map[string]interface{}, dest interface{}) error {
	inbetween, err := json.Marshal(source)
	if err != nil {
		return err
	}

	return json.Unmarshal(inbetween, dest)
}

//Pack packs a GO object into JSON-esque objects used in the Command and Event objects
func Pack(source interface{}) (map[string]interface{}, error) {
	dest := make(map[string]interface{})
	inbetween, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(inbetween, &dest)
	return dest, err
}
