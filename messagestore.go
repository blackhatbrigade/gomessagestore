package gomessagestore

import (
  "strings"
  "reflect"
  "database/sql"
  "context"
  "encoding/json"

  "github.com/sirupsen/logrus"
)

type MessageStore interface {
  WriteCommand(ctx context.Context, command *Command, position ...int64) error
  //WriteCommandWithPosition(ctx context.Context, command *Command, position ...int64) error
  ShutdownConnection()
}

type msgStore struct {
  db      *sql.DB
  repo    Repository
}

// Grabs a MessageStore instance.
func GetMessageStoreInterface() MessageStore {
  msgstr := new(msgStore)
  var err error

  msgstr.db, err = GetDBInstance()
  if err != nil {
    logrus.WithError(err).Panic("Problem getting the message store handle!")
    panic(err)
  }
  return msgstr
}

// Writes a Command to the message store.
func (ms *msgStore) WriteCommand(ctx context.Context, command *Command, position ...int64) error {
  err := validateCommand(command)
  if err != nil {
    return err
  }

  _, err = json.Marshal(command.Data)
	if err != nil {
		logrus.WithError(err).Error("Failure in to marshal command data")

		return ErrUnserializableData
	}

	if len(position) == 0 {
	//	return ms.repo.WriteMessage(ctx, )
	}

	//return ms.repo.WriteMessageWithExpectedPosition(ctx, message, position[0])
  return nil
}

// Shuts down the connection to the database.
// This should be deferred on messagestore instantiation.
func (ms *msgStore) ShutdownConnection() {
  ms.db.Close()
}

// Valids that the Command has the necessary data points.
func validateCommand(command *Command) error {
  if command.Type == "" {
    return ErrMissingMessageType
  }

  if command.Category == "" {
    return ErrMissingMessageCategory
  }

  if strings.Contains(command.Category, "-") {
    return ErrInvalidMessageCategory
  }

  if command.NewID == "" {
    return ErrMessageNoID
  }

  // TODO: Need to dig on this one some more...
  if command.Data == nil || (reflect.ValueOf(command.Data).Kind() == reflect.Ptr && reflect.ValueOf(command.Data).IsNil()) {
    return ErrMissingMessageData
  }

  return nil
}
