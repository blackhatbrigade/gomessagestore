package repository_test

import (
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestPostgresRepoFindAllMessagesInStream(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		expectedMessages []*MessageEnvelope
		expectedErr      error
		streamName       string
		callCancel       bool
		batchSize        int
	}{{
		name:             "when there are existing messages it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: copyAndAppend(mockMessages[:1], mockMessages[4:]...),
		batchSize:        1000,
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamName:       "some_other_type-23456",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		batchSize:        1000,
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamName:       "some_other_non_existant_type-124555",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamName:       "some_type-12345",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:        "when asking for messages from a stream with a blank ID, an error is returned",
		expectedErr: ErrInvalidStreamID,
		batchSize:   1000,
	}, {
		name:        "when asking for messages with a negative batch size, an error is returned",
		streamName:  "something-12345",
		expectedErr: ErrNegativeBatchSize,
		batchSize:   -10,
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		streamName:  "some_type-12345",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
		batchSize:   1000,
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_stream_messages\\(\\$1, \\$2\\)").
				WithArgs(test.streamName, 0).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamName == test.streamName {
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
			messages, err := repo.GetAllMessagesInStream(ctx, test.streamName, test.batchSize)

			assert.Equal(test.expectedMessages, messages)
			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoFindAllMessagesInStreamSince(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		messagesMetadata []string
		expectedMessages []*MessageEnvelope
		expectedErr      error
		streamName       string
		callCancel       bool
		position         int64
		batchSize        int
	}{{
		name:             "when there are existing messages past position -1 it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: copyAndAppend(mockMessages[:1], mockMessages[4:]...),
		position:         -1,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 0 it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: copyAndAppend(mockMessages[:1], mockMessages[4:]...),
		position:         0,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 5 it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: mockMessages[4:],
		position:         5,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages past position 10 it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: []*MessageEnvelope{},
		position:         10,
		batchSize:        1000,
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamName:       "some_other_type-23456",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
		batchSize:        1000,
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamName:       "some_other_non_existant_type-124555",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamName:       "some_type-12345",
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}, {
		name:        "when asking for messages from a stream with a blank ID, an error is returned",
		expectedErr: ErrInvalidStreamID,
		batchSize:   1000,
	}, {
		name:        "when asking for messages with a negative batch size, an error is returned",
		streamName:  "something-12345",
		expectedErr: ErrNegativeBatchSize,
		batchSize:   -10,
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		streamName:  "some_type-12345",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
		batchSize:   1000,
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
		batchSize:        1000,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_stream_messages\\(\\$1, \\$2\\)").
				WithArgs(test.streamName, test.position).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamName == test.streamName && row.GlobalPosition >= test.position {
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
			messages, err := repo.GetAllMessagesInStreamSince(ctx, test.streamName, test.position, test.batchSize)

			assert.Equal(test.expectedMessages, messages)
			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoFindLastMessageInStream(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		messagesMetadata []string
		expectedMessage  *MessageEnvelope
		expectedErr      error
		streamName       string
		callCancel       bool
	}{{
		name:             "when there are existing messages it should return the last one",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessage:  mockMessages[4],
	}, {
		name:             "when there are existing messages with bad metadata it should return the last one, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamName:       "some_other_type-23456",
		expectedMessage:  mockMessagesWithNoMetaData[0],
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
	}, {
		name:             "when there are no messages in my stream it should return no message",
		existingMessages: mockMessages,
		streamName:       "some_other_non_existant_type-124555",
	}, {
		name:       "when there are no existing messages it should return no message",
		streamName: "some_type-12345",
	}, {
		name:        "when asking for messages from a stream with a blank ID, an error is returned",
		expectedErr: ErrInvalidStreamID,
	}, {
		name:        "when there is an issue getting the message an error should be returned",
		streamName:  "some_type-12345",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		callCancel:       true,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_last_message\\(\\$1\\)").
				WithArgs(test.streamName).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				var lastRow *MessageEnvelope
				for _, row := range test.existingMessages {
					if row.StreamName == test.streamName {
						addedMessage++
						lastRow = row
					}
				}

				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				if lastRow != nil {
					rows.AddRow(
						lastRow.ID, lastRow.StreamName, lastRow.StreamCategory, lastRow.MessageType, lastRow.Version, lastRow.GlobalPosition, lastRow.Data, lastRow.Metadata, lastRow.Time,
					)
				}

				expectedQuery.WillReturnRows(rows)
			} else {
				expectedQuery.WillReturnError(test.dbError)
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			message, err := repo.GetLastMessageInStream(ctx, test.streamName)

			assert.Equal(test.expectedMessage, message)
			assert.Equal(test.expectedErr, err)
		})
	}
}
