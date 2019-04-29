package gomessagestore

import (
  "fmt"
  "database/sql"
)

type MessageStore interface {
  WriteCommand(ctx context.Context, command *Command) error
}

type msgStore struct {
  db *sql.DB
}

func GetMessageStoreInterface() *MessageStore {
  msgstr := new(msgStore)
  msgstr.db = GetDBInstance()
  return msgstr
}

func (ms *msgStore) WriteCommand(ctx context.Context, command *Command) error {
  fmt.Println("Message Store Working")
  return nil
}

func (ms *msgStore) ShutdownConnection() {
  ms.db.Close()
}
