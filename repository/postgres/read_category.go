package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

func (r postgresRepo) GetAllMessagesInCategory(ctx context.Context, category string, batchSize int) (m []*repository.MessageEnvelope, err error) {
	return r.GetAllMessagesInCategorySince(ctx, category, 0, batchSize)
}

func (r postgresRepo) GetAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64, batchSize int) (m []*repository.MessageEnvelope, err error) {
	if category == "" {
		logrus.WithError(repository.ErrBlankCategory).Error("Failure in repo_postgres.go::GetAllMessagesInCategorySince")

		return nil, repository.ErrBlankCategory
	}
	if batchSize < 0 {
		logrus.WithError(repository.ErrNegativeBatchSize).Error("Failure in repo_postgres.go::GetAllMessagesInCategorySince")

		return nil, repository.ErrNegativeBatchSize
	}
	if strings.Contains(category, "-") {
		logrus.WithError(repository.ErrInvalidCategory).Error("Failure in repo_postgres.go::GetAllMessagesInCategorySince")
		return nil, repository.ErrInvalidCategory
	}

	// our return channel for our goroutine that will either finish or be cancelled
	retChan := make(chan returnPair, 1)
	go func() {
		// last thing we do is ensure our return channel is populated
		defer func() {
			retChan <- returnPair{nil, nil}
		}()

		var msgs []*repository.MessageEnvelope
		/*get_category_messages(
		  _stream_name varchar,
		  _position bigint DEFAULT 0,
		  _batch_size bigint DEFAULT 1000,
		  _condition varchar DEFAULT NULL
		)*/

		query := "SELECT * FROM get_category_messages($1, $2, $3)"
		logrus.WithFields(map[string]interface{}{
			"query": query,
			"params": []string{
				category,
				fmt.Sprintf("%d", globalPosition),
				fmt.Sprintf("%d", batchSize),
			},
		}).Debug("Running query on DB")
		if err := r.dbx.SelectContext(ctx, &msgs, query, category, globalPosition, batchSize); err != nil {
			logrus.WithError(err).Error("Failure in repo_postgres.go::GetAllMessagesInCategorySince")
			retChan <- returnPair{nil, err}
			return
		}

		if len(msgs) == 0 {
			logrus.WithFields(map[string]interface{}{
				"category": category,
			}).Debug("read nothing from category")
			retChan <- returnPair{[]*repository.MessageEnvelope{}, nil}
			return
		}

		for _, msg := range msgs {
			logrus.WithFields(map[string]interface{}{
				"category": category,
			}).Debugf("read from category %s", msg.StreamName)
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
