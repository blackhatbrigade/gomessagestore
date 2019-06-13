package repository

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
)

//NewPostgresRepository creates a new in memory implementation for the messagestore reop
func NewPostgresRepository(db *sql.DB) Repository {
	r := new(postgresRepo)
	r.dbx = sqlx.NewDb(db, "postgres")
	r.subscriberIDToPosition = make(map[string]int64) // for now, start at the beginning of time, later, we'll make a place for this
	return r
}

type postgresRepo struct {
	dbx                    *sqlx.DB
	subscriberIDToPosition map[string]int64
}

type returnPair struct {
	messages []*MessageEnvelope
	err      error
}

//Actual values that come out of the database
type MessageEnvelope struct {
	ID             string    `db:"id"`              //MessageID
	StreamName     string    `db:"stream_name"`     //Stream
	StreamCategory string    `db:"stream_category"` //StreamType
	MessageType    string    `db:"type"`            //Type
	Version        int64     `db:"position"`
	GlobalPosition int64     `db:"global_position"`
	Data           []byte    `db:"data"`
	Metadata       []byte    `db:"metadata"` //eventideMessageMetaData
	Time           time.Time `db:"time"`
}

// MetaData {
//	CorrelationID string `json:"correlation_id,omitempty" db:"correlation_id"`
//	CausedByID    string `json:"caused_by_id,omitempty" db:"caused_by_id"`
//	UserID        string `json:"user_id,omitempty" db:"user_id"`
//}
