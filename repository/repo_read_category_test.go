package repository_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPostgresRepoFindAllMessagesInCategory(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		expectedMessages []*MessageEnvelope
		expectedErr      error
		streamCategory   string
		callCancel       bool
		batchSize        int
		logrusLogger     *logrus.Logger
	}{{
		name:             "when there are existing messages it should return them",
		existingMessages: mockMessages,
		streamCategory:   "other_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
		batchSize:        1000,
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamCategory:   "some_other_type",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		batchSize:        1000,
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamCategory:   "some_other_non_existant_type",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamCategory:   "other_type",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:           "when asking for messages from a stream with a invalid category, an error is returned",
		streamCategory: "something-with-a-hyphen",
		expectedErr:    ErrInvalidCategory,
		batchSize:      1000,
	}, {
		name:        "when asking for messages from a stream with a blank category, an error is returned",
		expectedErr: ErrBlankCategory,
		batchSize:   1000,
	}, {
		name:           "when asking for messages with a negative batch size, an error is returned",
		streamCategory: "something",
		expectedErr:    ErrNegativeBatchSize,
		batchSize:      -10,
	}, {
		name:           "when there is an issue getting the messages an error should be returned",
		streamCategory: "other_type",
		dbError:        errors.New("bad things with db happened"),
		expectedErr:    errors.New("bad things with db happened"),
		batchSize:      1000,
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamCategory:   "other_type",
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			logrusLogger := &logrus.Logger{
				Out:       os.Stderr,
				Formatter: new(logrus.JSONFormatter),
				Hooks:     make(logrus.LevelHooks),
				Level:     logrus.InfoLevel,
			}

			repo := NewPostgresRepository(db, logrusLogger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // free all resources

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_category_messages\\(\\$1, \\$2, \\$3\\)").
				WithArgs(test.streamCategory, 0, test.batchSize).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamCategory == test.streamCategory {
						addedMessage++
						rows.AddRow(
							row.ID, row.StreamName, row.StreamCategory, row.MessageType, row.Version, row.GlobalPosition, row.Data, row.Metadata, row.Time,
						)
					}
				}

				expectedQuery.WillReturnRows(rows)
			} else {
				expectedQuery.WillReturnError(test.dbError)
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			messages, err := repo.GetAllMessagesInCategory(ctx, test.streamCategory, test.batchSize)

			assert.Equal(test.expectedErr, err)
			assert.Equal(test.expectedMessages, messages)
		})
	}
}

func TestPostgresRepoFindAllMessagesInCategorySince(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		expectedMessages []*MessageEnvelope
		expectedErr      error
		streamType       string
		callCancel       bool
		position         int64
		batchSize        int
	}{{
		name:             "when there are existing messages past position -1 it should return them",
		existingMessages: mockMessages,
		streamType:       "other_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
		position:         -1,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 0 it should return them",
		existingMessages: mockMessages,
		streamType:       "other_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
		position:         0,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 5 it should return them",
		existingMessages: mockMessages,
		streamType:       "other_type",
		expectedMessages: mockMessages[4:],
		position:         5,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 10 it should return them",
		existingMessages: mockMessages,
		streamType:       "other_type",
		expectedMessages: []*MessageEnvelope{},
		position:         10,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamType:       "some_other_type",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		batchSize:        1000,
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamType:       "some_other_non_existant_type",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamType:       "other_type",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:        "when asking for messages from a category, if blank, an error is returned",
		expectedErr: ErrBlankCategory,
		batchSize:   1000,
	}, {
		name:        "when asking for messages with a negative batch size, an error is returned",
		streamType:  "something",
		expectedErr: ErrNegativeBatchSize,
		batchSize:   -10,
	}, {
		name:        "when asking for messages from a category, if is invalid, an error is returned",
		expectedErr: ErrInvalidCategory,
		streamType:  "something-bad",
		batchSize:   1000,
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		streamType:  "other_type",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
		batchSize:   1000,
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamType:       "other_type",
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			logrusLogger := &logrus.Logger{
				Out:       os.Stderr,
				Formatter: new(logrus.JSONFormatter),
				Hooks:     make(logrus.LevelHooks),
				Level:     logrus.InfoLevel,
			}
			repo := NewPostgresRepository(db, logrusLogger)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_category_messages\\(\\$1, \\$2, \\$3\\)").
				WithArgs(test.streamType, test.position, test.batchSize).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamCategory == test.streamType && row.GlobalPosition >= test.position {
						addedMessage++
						rows.AddRow(
							row.ID, row.StreamName, row.StreamCategory, row.MessageType, row.Version, row.GlobalPosition, row.Data, row.Metadata, row.Time,
						)
					}
				}

				expectedQuery.WillReturnRows(rows)
			} else {
				expectedQuery.WillReturnError(test.dbError)
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			messages, err := repo.GetAllMessagesInCategorySince(ctx, test.streamType, test.position, test.batchSize)

			assert.Equal(test.expectedErr, err)
			assert.Equal(test.expectedMessages, messages)
		})
	}
}
