package postgres

import (
	"context"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

func (r postgresRepo) WriteMessage(ctx context.Context, msg *repository.MessageEnvelope) error {
	return r.writeMessageEitherWay(ctx, msg)
}

func (r postgresRepo) WriteMessageWithExpectedPosition(ctx context.Context, msg *repository.MessageEnvelope, position int64) error {
	return r.writeMessageEitherWay(ctx, msg, position)
}

func (r postgresRepo) writeMessageEitherWay(ctx context.Context, msg *repository.MessageEnvelope, position ...int64) error {
	if msg == nil {
		return repository.ErrNilMessage
	}

	if msg.ID == uuid.Nil {
		return repository.ErrMessageNoID
	}

	if msg.StreamName == "" {
		return repository.ErrInvalidStreamName
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan error, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- nil
		}()

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
				retChan <- repository.ErrInvalidPosition
				return
			}

			// with _expected_version passed in
			query := "SELECT write_message($1, $2, $3, $4, $5, $6)"
			if _, err := r.dbx.ExecContext(ctx, query, msg.ID, msg.StreamName, msg.MessageType, msg.Data, msg.Metadata, position[0]); err != nil {
				logrus.WithError(err).Error("Failure in repo_postgres.go::WriteMessageWithExpectedPosition")
				retChan <- err
				return
			}
		} else {
			// without _expected_version passed in
			query := "SELECT write_message($1, $2, $3, $4, $5)"
			logrus.WithFields(logrus.Fields{
				"query":              query,
				"ID":                 msg.ID,
				"StreamName":         msg.StreamName,
				"MessageMessageType": msg.MessageType,
				"Data":               string(msg.Data),
				"MessageMetadata":    string(msg.Metadata),
			}).Debug("about to write message")
			if _, err := r.dbx.ExecContext(ctx, query, msg.ID, msg.StreamName, msg.MessageType, msg.Data, msg.Metadata); err != nil {
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
