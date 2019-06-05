package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func metadataJSON(message *MessageEnvelope) []byte {
	metadata := struct {
		CorrelationID string `json:"correlation_id,omitempty" db:"correlation_id"`
		CausedByID    string `json:"caused_by_id,omitempty" db:"caused_by_id"`
		UserID        string `json:"user_id,omitempty" db:"user_id"`
	}{message.CorrelationID, message.CausedByID, message.UserID}
	bytes, _ := json.Marshal(metadata)

	return bytes
}

var mockMessagesWithNoMetaData = []*MessageEnvelope{{
	GlobalPosition: 5,
	MessageID:      "dag-2346",
	Type:           "some_other_type",
	Stream:         "some_other_type-23456",
	StreamType:     "some_other_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       0,
	Data:           []byte("{d:\"a\"}"),
	Timestamp:      time.Unix(1546773907, 0),
}, {
	GlobalPosition: 6,
	MessageID:      "daf-3346",
	Type:           "some_other_other_type",
	Stream:         "some_other_other_type-23456",
	StreamType:     "some_other_other_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       0,
	Data:           []byte("{d:\"a\"}"),
	Timestamp:      time.Unix(1546773907, 0),
}, {
	GlobalPosition: 7,
	MessageID:      "abc-456",
	Type:           "some_type",
	Stream:         "some_type-12345",
	StreamType:     "some_type",
	CorrelationID:  "qwerty-asdfg-some-real-guid",
	CausedByID:     "",
	UserID:         "hjklm",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}}

var mockMessageNoID = &MessageEnvelope{
	GlobalPosition: 7,
	Type:           "some_type",
	Stream:         "some_type-12345",
	StreamType:     "some_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}

var mockMessageNoStream = &MessageEnvelope{
	GlobalPosition: 7,
	MessageID:      "abc-456",
	Type:           "some_type",
	StreamType:     "some_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}

func TestPostgresRepoFetchAllMessagesSince(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		messagesMetadata []string
		expectedMessages []*MessageEnvelope
		expectedErr      error
		position         int64
		callCancel       bool
	}{{
		name:             "when there are existing messages it should return them",
		existingMessages: mockMessages,
		position:         4,
		expectedMessages: mockMessages[2:],
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		position:         4,
		expectedMessages: mockMessagesWithNoMetaData,
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
	}, {
		name:             "when there are no messages past position it should return no messages",
		position:         10,
		expectedMessages: []*MessageEnvelope{},
	}, {
		name:             "when there are no existing messages it should return no messages",
		expectedMessages: []*MessageEnvelope{},
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		position:         4,
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT id, stream_name, category\\(stream_name\\) AS stream_category, type, position, global_position, data, metadata, time FROM messages WHERE global_position > \\$1 LIMIT 100").
				WithArgs(test.position).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.GlobalPosition > test.position {
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
			messages, err := repo.FindAllMessagesSince(ctx, test.position)

			assert.Equal(test.expectedMessages, messages)
			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoFetchAllMessagesInStream(t *testing.T) {
	tests := []struct {
		name             string
		dbError          error
		existingMessages []*MessageEnvelope
		messagesMetadata []string
		expectedMessages []*MessageEnvelope
		expectedErr      error
		streamName       string
		callCancel       bool
	}{{
		name:             "when there are existing messages it should return them",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		expectedMessages: copyAndAppend(mockMessages[:1], mockMessages[4:]...),
	}, {
		name:             "when there are existing messages with bad metadata it should return them, ignoring the bad metadata",
		existingMessages: mockMessages,
		streamName:       "some_other_type-23456",
		expectedMessages: mockMessagesWithNoMetaData[:1],
		messagesMetadata: []string{"this isn't JSON", "{\"alternate\":\"json\"}"},
	}, {
		name:             "when there are no messages in my stream it should return no messages",
		existingMessages: mockMessages,
		streamName:       "some_other_non_existant_type-124555",
		expectedMessages: []*MessageEnvelope{},
	}, {
		name:             "when there are no existing messages it should return no messages",
		streamName:       "some_type-12345",
		expectedMessages: []*MessageEnvelope{},
	}, {
		name:        "when asking for messages from a stream with a blank ID, an error is returned",
		expectedErr: ErrInvalidStreamID,
	}, {
		name:        "when there is an issue getting the messages an error should be returned",
		streamName:  "some_type-12345",
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:             "when it is asked to cancel, it does",
		existingMessages: mockMessages,
		streamName:       "some_type-12345",
		callCancel:       true,
		expectedMessages: []*MessageEnvelope{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			expectedQuery := mockDb.
				ExpectQuery("SELECT \\* FROM get_stream_messages\\(\\$1\\)").
				WithArgs(test.streamName).
				WillDelayFor(time.Millisecond * 10)

			addedMessage := -1
			if test.dbError == nil {
				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				for _, row := range test.existingMessages {
					if row.Stream == test.streamName {
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
			messages, err := repo.FindAllMessagesInStream(ctx, test.streamName)

			assert.Equal(test.expectedMessages, messages)
			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoFetchLastMessageInStream(t *testing.T) {
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
				lastMetadata := ""
				for _, row := range test.existingMessages {
					if row.Stream == test.streamName {
						addedMessage++
						if len(test.messagesMetadata) > addedMessage {
							lastMetadata = test.messagesMetadata[addedMessage]
						} else {
							lastMetadata = fmt.Sprintf("{\"correlation_id\":\"%s\", \"caused_by_id\":\"%s\", \"user_id\":\"%s\"}", row.CorrelationID, row.CausedByID, row.UserID)
						}
						lastRow = row
					}
				}

				rows := sqlmock.NewRows([]string{"id", "stream_name", "stream_category", "type", "position", "global_position", "data", "metadata", "time"})
				if lastRow != nil {
					rows.AddRow(
						lastRow.MessageID, lastRow.Stream, lastRow.StreamType, lastRow.Type, lastRow.Position, lastRow.GlobalPosition, lastRow.Data, lastMetadata, lastRow.Timestamp,
					)
				}

				expectedQuery.WillReturnRows(rows)
			} else {
				expectedQuery.WillReturnError(test.dbError)
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			message, err := repo.FindLastMessageInStream(ctx, test.streamName)

			assert.Equal(test.expectedMessage, message)
			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoFindAndSetSubscriberPosition(t *testing.T) {
	tests := []struct {
		name                  string
		expectedSetErr        error
		expectedFindErr       error
		subscriberID          string
		subscriberPosition    int64
		expectedFoundPosition int64
	}{{
		name:                  "when a position is 0, no errors are returned",
		subscriberID:          "service:test:action",
		subscriberPosition:    0,
		expectedFoundPosition: 0,
	}, {
		name:                  "when a position is positive, no error is returned",
		subscriberID:          "service:test:action",
		subscriberPosition:    5,
		expectedFoundPosition: 5,
	}, {
		name:                  "when a position is really big number, no error is returned",
		subscriberID:          "service:test:action",
		subscriberPosition:    5000000000,
		expectedFoundPosition: 5000000000,
	}, {
		name:                  "when setting position to less than -1, an error is returned, but the position is still found at -1",
		subscriberID:          "service:test:action",
		subscriberPosition:    -5,
		expectedSetErr:        ErrInvalidSubscriberPosition,
		expectedFoundPosition: -1,
	}, {
		name:                  "when setting position to -1, no error is returned",
		subscriberID:          "service:test:action",
		subscriberPosition:    -1,
		expectedFoundPosition: -1,
	}, {
		name:                  "when the subscriber ID is blank, an error is returned on finding and setting",
		expectedFindErr:       ErrInvalidSubscriberID,
		expectedSetErr:        ErrInvalidSubscriberID,
		expectedFoundPosition: -1,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, _, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, _ := context.WithCancel(context.Background())

			err := repo.SetSubscriberPosition(ctx, test.subscriberID, test.subscriberPosition)
			assert.Equal(test.expectedSetErr, err)
			foundPosition, err := repo.FindSubscriberPosition(ctx, test.subscriberID)
			assert.Equal(test.expectedFoundPosition, foundPosition)
			assert.Equal(test.expectedFindErr, err)
		})
	}
}

func TestPostgresRepoWriteMessage(t *testing.T) {
	tests := []struct {
		name        string
		message     *MessageEnvelope
		dbError     error
		expectedErr error
		callCancel  bool
	}{{
		name:        "when there is a db error, return it",
		message:     mockMessages[0],
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:        "when there is a nil message, an error is returned",
		expectedErr: ErrNilMessage,
	}, {
		name:        "when the message has no ID, an error is returned",
		message:     mockMessageNoID,
		expectedErr: ErrMessageNoID,
	}, {
		name:        "when the message has no stream name, an error is returned",
		message:     mockMessageNoStream,
		expectedErr: ErrInvalidStreamID,
	}, {
		name:    "when there is no db error, it should write the message",
		message: mockMessages[0],
	}, {
		name:       "when it is asked to cancel, it does",
		message:    mockMessages[0],
		callCancel: true,
		dbError:    errors.New("this shouldn't be returned, because we're cancelling"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			if test.message != nil {
				expectedExec := mockDb.
					ExpectExec("SELECT write_message\\(\\$1, \\$2, \\$3, \\$4, \\$5\\)").
					WithArgs(test.message.MessageID,
						test.message.Stream,
						test.message.Type,
						test.message.Data,
						metadataJSON(test.message)).
					WillDelayFor(time.Millisecond * 10)

				if test.dbError == nil {
					expectedExec.WillReturnResult(sqlmock.NewResult(1, 1))
				} else {
					expectedExec.WillReturnError(test.dbError)
				}
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			err := repo.WriteMessage(ctx, test.message)

			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoWriteMessageWithExpectedPosition(t *testing.T) {
	tests := []struct {
		name        string
		message     *MessageEnvelope
		dbError     error
		expectedErr error
		position    int64
		callCancel  bool
	}{{
		name:        "when there is a db error, return it",
		message:     mockMessages[0],
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
		position:    1,
	}, {
		name:        "when there is a nil message, an error is returned",
		expectedErr: ErrNilMessage,
		position:    1,
	}, {
		name:        "when the message has no ID, an error is returned",
		message:     mockMessageNoID,
		expectedErr: ErrMessageNoID,
		position:    1,
	}, {
		name:        "when the message has no stream name, an error is returned",
		message:     mockMessageNoStream,
		expectedErr: ErrInvalidStreamID,
		position:    1,
	}, {
		name:     "when the position is at 0, no error is returned",
		message:  mockMessages[0],
		position: 0,
	}, {
		name:     "when the position is at -1, no error is returned",
		message:  mockMessages[0],
		position: -1,
	}, {
		name:        "when the position is below -1, an error is returned",
		message:     mockMessages[0],
		expectedErr: ErrInvalidPosition,
		position:    -2,
	}, {
		name:     "when there is no db error, it should write the message",
		message:  mockMessages[0],
		position: 1,
	}, {
		name:       "when it is asked to cancel, it does",
		message:    mockMessages[0],
		position:   0,
		callCancel: true,
		dbError:    errors.New("this shouldn't be returned, because we're cancelling"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			repo := NewPostgresRepository(db)
			ctx, cancel := context.WithCancel(context.Background())

			if test.message != nil {
				expectedExec := mockDb.
					ExpectExec("SELECT write_message\\(\\$1, \\$2, \\$3, \\$4, \\$5, \\$6\\)").
					WithArgs(test.message.MessageID,
						test.message.Stream,
						test.message.Type,
						test.message.Data,
						metadataJSON(test.message),
						test.position).
					WillDelayFor(time.Millisecond * 10)

				if test.dbError == nil {
					expectedExec.WillReturnResult(sqlmock.NewResult(1, 1))
				} else {
					expectedExec.WillReturnError(test.dbError)
				}
			}

			if test.callCancel {
				time.AfterFunc(time.Millisecond*5, cancel) // after the call to the DB, but before it finishes
			}
			err := repo.WriteMessageWithExpectedPosition(ctx, test.message, test.position)

			assert.Equal(test.expectedErr, err)
		})
	}
}

