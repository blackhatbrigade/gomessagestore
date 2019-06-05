package repository

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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

func (r postgresRepo) FindAllMessagesSince(ctx context.Context, position int64) ([]*MessageEnvelope, error) {
	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var eventideMessages []*eventideMessageEnvelope
		query := "SELECT id, stream_name, category(stream_name) AS stream_category, type, position, global_position, data, metadata, time FROM messages WHERE global_position > $1 LIMIT 100"
		if err := r.dbx.SelectContext(ctx, &eventideMessages, query, position); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::FindAllMessagesSince")
			retChan <- returnPair{nil, err}
		}

		if len(eventideMessages) == 0 {
			retChan <- returnPair{[]*MessageEnvelope{}, nil}
			return
		}

		messages := r.translateMessages(eventideMessages)
		retChan <- returnPair{messages, nil}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		return retval.messages, retval.err
	case <-ctx.Done():
		return []*MessageEnvelope{}, nil
	}
}

func (r postgresRepo) FindAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error) {
	if streamID == "" {
		logrus.WithError(ErrInvalidStreamID).Error("Failure in repo_postgres.go::FindAllMessagesInStream")

		return nil, ErrInvalidStreamID
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var eventideMessages []*eventideMessageEnvelope
		/*get_stream_messages(
		  _stream_name varchar,
		  _position bigint DEFAULT 0,
		  _batch_size bigint DEFAULT 1000,
		  _condition varchar DEFAULT NULL
		)*/
		query := "SELECT * FROM get_stream_messages($1)"
		if err := r.dbx.SelectContext(ctx, &eventideMessages, query, streamID); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::FindAllMessagesInStream")
			retChan <- returnPair{nil, err}
			return
		}

		if len(eventideMessages) == 0 {
			retChan <- returnPair{[]*MessageEnvelope{}, nil}
			return
		}

		messages := r.translateMessages(eventideMessages)
		retChan <- returnPair{messages, nil}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		return retval.messages, retval.err
	case <-ctx.Done():
		return []*MessageEnvelope{}, nil
	}
}

func (r postgresRepo) FindLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error) {
	if streamID == "" {
		logrus.WithError(ErrInvalidStreamID).Error("Failure in repo_postgres.go::FindLastMessageInStream")

		return nil, ErrInvalidStreamID
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var eventideMessages []*eventideMessageEnvelope
		/*get_last_message(
		  _stream_name varchar,
		)*/
		query := "SELECT * FROM get_last_message($1)"
		if err := r.dbx.SelectContext(ctx, &eventideMessages, query, streamID); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::FindLastMessageInStream")
			retChan <- returnPair{nil, err}
			return
		}

		if len(eventideMessages) == 0 {
			retChan <- returnPair{[]*MessageEnvelope{nil}, nil}
			return
		}

		messages := r.translateMessages(eventideMessages)

		retChan <- returnPair{messages, nil}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		if retval.err != nil {
			return nil, retval.err
		} else if len(retval.messages) > 0 {
			return retval.messages[0], retval.err
		}
		return nil, nil
	case <-ctx.Done():
		return nil, nil
	}
}

func (r postgresRepo) FindSubscriberPosition(ctx context.Context, subscriberID string) (int64, error) {
	if subscriberID == "" {
		logrus.WithError(ErrInvalidSubscriberID).Error("Failure in repo_postgres.go::FindSubscriberPosition")

		return -1, ErrInvalidSubscriberID
	}

	position, ok := r.subscriberIDToPosition[subscriberID]
	if !ok {
		// TODO! load subscriber position from stream

		r.subscriberIDToPosition[subscriberID] = -1
		return -1, nil
	}

	return position, nil
}

func (r postgresRepo) SetSubscriberPosition(ctx context.Context, subscriberID string, position int64) error {
	if subscriberID == "" {
		logrus.WithError(ErrInvalidSubscriberID).Error("Failure in repo_postgres.go::SetSubscriberPosition")

		return ErrInvalidSubscriberID
	}

	if position < -1 {
		logrus.WithError(ErrInvalidSubscriberPosition).Error("Failure in repo_postgres.go::SetSubscriberPosition")

		return ErrInvalidSubscriberPosition
	}

	oldPosition, ok := r.subscriberIDToPosition[subscriberID]
	if !ok || oldPosition != position {
		r.subscriberIDToPosition[subscriberID] = position

		// TODO! write subscriber position to stream
	}

	return nil
}

func (r postgresRepo) WriteMessage(ctx context.Context, message *MessageEnvelope) error {
	return r.writeMessageEitherWay(ctx, message)
}

func (r postgresRepo) WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error {
	return r.writeMessageEitherWay(ctx, message, position)
}

func (r postgresRepo) writeMessageEitherWay(ctx context.Context, message *MessageEnvelope, position ...int64) error {
	if message == nil {
		return ErrNilMessage
	}

	if message.MessageID == "" {
		return ErrMessageNoID
	}

	if message.Stream == "" {
		return ErrInvalidStreamID
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan error, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- nil
		}()

		eventideMetadata := &eventideMessageMetadata{
			CorrelationID: message.CorrelationID,
			CausedByID:    message.CausedByID,
			UserID:        message.UserID,
		}
		eventideMessage := &eventideMessageEnvelope{
			ID:          message.MessageID,
			MessageType: message.Type,
			StreamName:  message.Stream,
			Data:        message.Data,
			Position:    message.Position,
		}

		if metadata, err := json.Marshal(eventideMetadata); err == nil {
			eventideMessage.Metadata = metadata
		} else {
			logrus.WithError(err).Error("Failure to marshal metadata in repo_postgres.go::WriteMessage")
			retChan <- err
			return
		}

		/*"write_message(
			_id varchar,
			_stream_name varchar,
			_type varchar,
			_data jsonb,
			_metadata jsonb DEFAULT NULL,
			_expected_version bigint DEFAULT NULL
		)"*/
		if len(position) > 0 {
			if position[0] < -1 {
				retChan <- ErrInvalidPosition
				return
			}

			// with _expected_version passed in
			query := "SELECT write_message($1, $2, $3, $4, $5, $6)"
			if _, err := r.dbx.ExecContext(ctx, query, eventideMessage.ID, eventideMessage.StreamName, eventideMessage.MessageType, eventideMessage.Data, eventideMessage.Metadata, position[0]); err != nil {
				logrus.WithError(err).Error("Failure in repo_postgres.go::WriteMessageWithExpectedPosition")
				retChan <- err
				return
			}
		} else {
			// without _expected_version passed in
			query := "SELECT write_message($1, $2, $3, $4, $5)"
			logrus.WithFields(logrus.Fields{
				"query":                      query,
				"eventideMessageID":          eventideMessage.ID,
				"eventideMessageStreamName":  eventideMessage.StreamName,
				"eventideMessageMessageType": eventideMessage.MessageType,
				"eventideMessageData":        eventideMessage.Data,
				"eventideMessageMetadata":    eventideMessage.Metadata,
			}).Debug("about to write message")
			if _, err := r.dbx.ExecContext(ctx, query, eventideMessage.ID, eventideMessage.StreamName, eventideMessage.MessageType, eventideMessage.Data, eventideMessage.Metadata); err != nil {
				logrus.WithError(err).Error("Failure in repo_postgres.go::WriteMessage")
				retChan <- err
				return
			}
		}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		return retval
	case <-ctx.Done():
		return nil
	}
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


