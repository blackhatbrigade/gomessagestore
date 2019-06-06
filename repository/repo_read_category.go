package repository

import (
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func (r postgresRepo) FindAllMessagesInCategory(ctx context.Context, category string) (m []*MessageEnvelope, err error) {
	return r.FindAllMessagesInCategorySince(ctx, category, 0)
}

func (r postgresRepo) FindAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64) (m []*MessageEnvelope, err error) {
	if category == "" {
		logrus.WithError(ErrBlankCategory).Error("Failure in repo_postgres.go::FindAllMessagesInCategorySince")

		return nil, ErrBlankCategory
	}
	if strings.Contains(category, "-") {
		logrus.WithError(ErrInvalidCategory).Error("Failure in repo_postgres.go::FindAllMessagesInCategorySince")
		return nil, ErrInvalidCategory
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var eventideMessages []*eventideMessageEnvelope
		/*get_category_messages(
		  _stream_name varchar,
		  _position bigint DEFAULT 0,
		  _batch_size bigint DEFAULT 1000,
		  _condition varchar DEFAULT NULL
		)*/
		query := "SELECT * FROM get_category_messages($1, $2)"
		if err := r.dbx.SelectContext(ctx, &eventideMessages, query, category, globalPosition); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::FindAllMessagesInCategorySince")
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
