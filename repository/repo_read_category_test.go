package repository

import (
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestPostgresRepoFindAllMessagesInCategory(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*message.MessageEnvelope
		messagesMetadata []string
		expectedMessages []*message.MessageEnvelope
		expectedErr      error
		category         string
		callCancel       bool
	}{{
		name:             "when there are existing messages it should return them",
		existingMessages: mockMessages,
		category:         "some_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		category:         "some_other_type",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		category:         "some_other_non_existant_type",
		expectedMessages: []*message.MessageEnvelope{},
	}, {
		name:             "when there are no existing messages it should return no messages",
		category:         "some_type",
		expectedMessages: []*message.MessageEnvelope{},
	}, {
		name:        "when asking for messages from a stream with a invalid category, an error is returned",
		category:    "something-with-a-hyphen",
		expectedErr: ErrInvalidCategory,
	}, {
		name:        "when asking for messages from a stream with a blank category, an error is returned",
		expectedErr: ErrBlankCategory,
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		category:    "some_type",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		category:         "some_type",
		callCancel:       true,
		expectedMessages: []*message.MessageEnvelope{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_category_messages\\(\\$1, \\$2\\)").
				WithArgs(test.category, 0).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamType == test.category {
						addedMessage++
						var metadata string
						if len(test.messagesMetadata) > addedMessage {
							metadata = test.messagesMetadata[addedMessage]
						} else {
							metadata = fmt.Sprintf("{\"correlation_id\":\"%s\", \"caused_by_id\":\"%s\", \"user_id\":\"%s\"}", row.CorrelationID, row.CausedByID, row.UserID)
						}
						rows.AddRow(
							row.MessageID, row.Stream, row.StreamType, row.Type, row.Position, row.GlobalPosition, row.Data, metadata, row.Timestamp,
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
			messages, err := repo.FindAllMessagesInCategory(ctx, test.category)

			assert.Equal(test.expectedErr, err)
			assert.Equal(test.expectedMessages, messages)
		})
	}
}

func TestPostgresRepoFindAllMessagesInCategorySince(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*message.MessageEnvelope
		messagesMetadata []string
		expectedMessages []*message.MessageEnvelope
		expectedErr      error
		streamType       string
		callCancel       bool
		position         int64
	}{{
		name:             "when there are existing messages past position -1 it should return them",
		existingMessages: mockMessages,
		streamType:       "some_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
		position:         -1,
	}, {
		name:             "when there are existing messages past position 0 it should return them",
		existingMessages: mockMessages,
		streamType:       "some_type",
		expectedMessages: copyAndAppend(mockMessages[:2], mockMessages[4:]...),
		position:         0,
	}, {
		name:             "when there are existing messages past position 5 it should return them",
		existingMessages: mockMessages,
		streamType:       "some_type",
		expectedMessages: mockMessages[4:],
		position:         5,
	}, {
		name:             "when there are existing messages past position 10 it should return them",
		existingMessages: mockMessages,
		streamType:       "some_type",
		expectedMessages: []*message.MessageEnvelope{},
		position:         10,
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamType:       "some_other_type",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamType:       "some_other_non_existant_type",
		expectedMessages: []*message.MessageEnvelope{},
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamType:       "some_type",
		expectedMessages: []*message.MessageEnvelope{},
	}, {
		name:        "when asking for messages from a category, if blank, an error is returned",
		expectedErr: ErrBlankCategory,
	}, {
		name:        "when asking for messages from a category, if is invalid, an error is returned",
		expectedErr: ErrInvalidCategory,
		streamType:  "something-bad",
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		streamType:  "some_type",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamType:       "some_type",
		callCancel:       true,
		expectedMessages: []*message.MessageEnvelope{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_category_messages\\(\\$1, \\$2\\)").
				WithArgs(test.streamType, test.position).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.StreamType == test.streamType && row.GlobalPosition >= test.position {
						addedMessage++
						var metadata string
						if len(test.messagesMetadata) > addedMessage {
							metadata = test.messagesMetadata[addedMessage]
						} else {
							metadata = fmt.Sprintf("{\"correlation_id\":\"%s\", \"caused_by_id\":\"%s\", \"user_id\":\"%s\"}", row.CorrelationID, row.CausedByID, row.UserID)
						}
						rows.AddRow(
							row.MessageID, row.Stream, row.StreamType, row.Type, row.Position, row.GlobalPosition, row.Data, metadata, row.Timestamp,
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
			messages, err := repo.FindAllMessagesInCategorySince(ctx, test.streamType, test.position)

			assert.Equal(test.expectedErr, err)
			assert.Equal(test.expectedMessages, messages)
		})
	}
}
