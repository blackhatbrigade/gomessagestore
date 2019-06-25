package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

//NewPostgresRepository creates a new in memory implementation for the messagestore reop
func NewPostgresRepository(db *sql.DB) Repository {
	r := new(postgresRepo)
	r.dbx = sqlx.NewDb(db, "postgres")
	return r
}

type postgresRepo struct {
	dbx *sqlx.DB
}

type returnPair struct {
	messages []*MessageEnvelope
	err      error
}

//Actual values that come out of the database
type MessageEnvelope struct {
	ID             string    `db:"id"`
	StreamName     string    `db:"stream_name"`
	StreamCategory string    `db:"stream_category"`
	MessageType    string    `db:"type"`
	Version        int64     `db:"position"`
	GlobalPosition int64     `db:"global_position"`
	Data           []byte    `db:"data"`
	Metadata       []byte    `db:"metadata"`
	Time           time.Time `db:"time"`
}

func (msgEnv *MessageEnvelope) String() string {
	return fmt.Sprintf("GlobalPosition: %d | ID: %s\nMessageType: %s | StreamName: %s | StreamCategory: %s", msgEnv.GlobalPosition, msgEnv.ID, msgEnv.MessageType, msgEnv.StreamName, msgEnv.StreamCategory)
}
