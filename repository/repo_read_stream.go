package repository

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func (r postgresRepo) GetAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error) {
	return r.GetAllMessagesInStreamSince(ctx, streamID, 0)
}

func (r postgresRepo) GetLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error) {
	if streamID == "" {
		logrus.WithError(ErrInvalidStreamID).Error("Failure in repo_postgres.go::GetLastMessageInStream")

		return nil, ErrInvalidStreamID
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var msgs []*MessageEnvelope
		/*get_last_message(
		  _stream_name varchar,
		)*/
		query := "SELECT * FROM get_last_message($1)"
		if err := r.dbx.SelectContext(ctx, &msgs, query, streamID); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::GetLastMessageInStream")
			retChan <- returnPair{nil, err}
			return
		}

		if len(msgs) == 0 {
			retChan <- returnPair{[]*MessageEnvelope{nil}, nil}
			return
		}

		retChan <- returnPair{msgs, nil}
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

func (r postgresRepo) GetAllMessagesInStreamSince(ctx context.Context, streamID string, globalPosition int64) ([]*MessageEnvelope, error) {
	if streamID == "" {
		logrus.WithError(ErrInvalidStreamID).Error("Failure in repo_postgres.go::GetAllMessagesInStreamSince")

		return nil, ErrInvalidStreamID
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var msgs []*MessageEnvelope
		/*get_stream_messages(
		  _stream_name varchar,
		  _position bigint DEFAULT 0,
		  _batch_size bigint DEFAULT 1000,
		  _condition varchar DEFAULT NULL
		)*/
		query := "SELECT * FROM get_stream_messages($1, $2)"
		if err := r.dbx.SelectContext(ctx, &msgs, query, streamID, globalPosition); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::GetAllMessagesInStreamSince")
			retChan <- returnPair{nil, err}
			return
		}

		if len(msgs) == 0 {
			retChan <- returnPair{[]*MessageEnvelope{}, nil}
			return
		}

		retChan <- returnPair{msgs, nil}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		return retval.messages, retval.err
	case <-ctx.Done():
		return []*MessageEnvelope{}, nil
	}
}
