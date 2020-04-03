package postgres

import (
	"context"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

func (r postgresRepo) GetAllMessagesInStream(ctx context.Context, streamName string, batchSize int) ([]*repository.MessageEnvelope, error) {
	return r.GetAllMessagesInStreamSince(ctx, streamName, 0, batchSize)
}

func (r postgresRepo) GetLastMessageInStream(ctx context.Context, streamName string) (*repository.MessageEnvelope, error) {
	if streamName == "" {
		logrus.WithError(repository.ErrInvalidStreamName).Error("Failure in repo_postgres.go::GetLastMessageInStream")

		return nil, repository.ErrInvalidStreamName
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var msgs []*repository.MessageEnvelope
		/*get_last_message(
		  _stream_name varchar,
		)*/
		query := "SELECT * FROM get_last_message($1)"
		if err := r.dbx.SelectContext(ctx, &msgs, query, streamName); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::GetLastMessageInStream")
			retChan <- returnPair{nil, err}
			return
		}

		if len(msgs) == 0 {
			retChan <- returnPair{[]*repository.MessageEnvelope{nil}, nil}
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

func (r postgresRepo) GetAllMessagesInStreamSince(ctx context.Context, streamName string, globalPosition int64, batchSize int) ([]*repository.MessageEnvelope, error) {
	if streamName == "" {
		logrus.WithError(repository.ErrInvalidStreamName).Error("Failure in repo_postgres.go::GetAllMessagesInStreamSince")

		return nil, repository.ErrInvalidStreamName
	}
	if batchSize < 0 {
		logrus.WithError(repository.ErrNegativeBatchSize).Error("Failure in repo_postgres.go::GetAllMessagesInCategorySince")

		return nil, repository.ErrNegativeBatchSize
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var msgs []*repository.MessageEnvelope
		/*get_stream_messages(
		  _stream_name varchar,
		  _position bigint DEFAULT 0,
		  _batch_size bigint DEFAULT 1000,
		  _condition varchar DEFAULT NULL
		)*/
		query := "SELECT * FROM get_stream_messages($1, $2)"
		if err := r.dbx.SelectContext(ctx, &msgs, query, streamName, globalPosition); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::GetAllMessagesInStreamSince")
			retChan <- returnPair{nil, err}
			return
		}

		if len(msgs) == 0 {
			retChan <- returnPair{[]*repository.MessageEnvelope{}, nil}
			return
		}

		retChan <- returnPair{msgs, nil}
	}()

	// wait for our return channel or the context to cancel
	select {
	case retval := <-retChan:
		return retval.messages, retval.err
	case <-ctx.Done():
		return []*repository.MessageEnvelope{}, nil
	}
}
