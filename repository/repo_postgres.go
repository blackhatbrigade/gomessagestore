package repository

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
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

type eventideMessageEnvelope struct {
	ID             string    `db:"id"`
	StreamName     string    `db:"stream_name"`
	StreamCategory string    `db:"stream_category"`
	MessageType    string    `db:"type"`
	Position       int64     `db:"position"`
	GlobalPosition int64     `db:"global_position"`
	Data           []byte    `db:"data"`
	Metadata       []byte    `db:"metadata"`
	Time           time.Time `db:"time"`
}

type eventideMessageMetadata struct {
	CorrelationID string `json:"correlation_id,omitempty" db:"correlation_id"`
	CausedByID    string `json:"caused_by_id,omitempty" db:"caused_by_id"`
	UserID        string `json:"user_id,omitempty" db:"user_id"`
}

type returnPair struct {
	messages []*MessageEnvelope
	err      error
}

func (r postgresRepo) translateMessages(eventideMessages []*eventideMessageEnvelope) []*MessageEnvelope {
	messages := make([]*MessageEnvelope, len(eventideMessages))
	for index, eventideMessage := range eventideMessages {
		messages[index] = &MessageEnvelope{
			GlobalPosition: eventideMessage.GlobalPosition,
			MessageID:      eventideMessage.ID,
			Type:           eventideMessage.MessageType,
			Stream:         eventideMessage.StreamName,
			StreamType:     eventideMessage.StreamCategory,
			Data:           eventideMessage.Data,
			Position:       eventideMessage.Position,
			Timestamp:      eventideMessage.Time,
		}

		metadata := &eventideMessageMetadata{}
		if err := json.Unmarshal(eventideMessage.Metadata, metadata); err == nil {
			messages[index].CorrelationID = metadata.CorrelationID
			messages[index].CausedByID = metadata.CausedByID
			messages[index].UserID = metadata.UserID
		} else {
			// if there's an error here, log but ignore it: poorly formed metadata shouldn't break our flow
			logrus.WithError(err).Error("Failure to parse metadata in repo_postgres.go::translateMessages")
		}
	}

	return messages
}
