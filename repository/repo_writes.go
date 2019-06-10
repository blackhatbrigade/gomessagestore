package repository

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/blackhatbrigade/gomessagestore/message"
)

func (r postgresRepo) WriteMessage(ctx context.Context, msg *message.MessageEnvelope) error {
	return r.writeMessageEitherWay(ctx, msg)
}

func (r postgresRepo) WriteMessageWithExpectedPosition(ctx context.Context, msg *message.MessageEnvelope, position int64) error {
	return r.writeMessageEitherWay(ctx, msg, position)
}

func (r postgresRepo) writeMessageEitherWay(ctx context.Context, msg *message.MessageEnvelope, position ...int64) error {
	if msg == nil {
		return ErrNilMessage
	}

	if msg.MessageID == "" {
		return message.ErrMessageNoID
	}

	if msg.Stream == "" {
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
			CorrelationID: msg.CorrelationID,
			CausedByID:    msg.CausedByID,
			UserID:        msg.UserID,
		}
		eventideMessage := &eventideMessageEnvelope{
			ID:          msg.MessageID,
			MessageType: msg.Type,
			StreamName:  msg.Stream,
			Data:        msg.Data,
			Position:    msg.Position,
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
