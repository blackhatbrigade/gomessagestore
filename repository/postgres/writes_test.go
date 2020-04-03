package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/blackhatbrigade/gomessagestore/repository"
	. "github.com/blackhatbrigade/gomessagestore/repository/postgres"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPostgresRepoWriteMessage(t *testing.T) {
	tests := []struct {
		name         string
		msg          *repository.MessageEnvelope
		dbError      error
		expectedErr  error
		callCancel   bool
		logrusLogger *logrus.Logger
	}{{
		name:        "when there is a db error, return it",
		msg:         mockMessages[0],
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
	}, {
		name:        "when there is a nil message, an error is returned",
		expectedErr: repository.ErrNilMessage,
	}, {
		name:        "when the message has no ID, an error is returned",
		msg:         mockMessageNoID,
		expectedErr: repository.ErrMessageNoID,
	}, {
		name:        "when the message has no stream name, an error is returned",
		msg:         mockMessageNoStream,
		expectedErr: repository.ErrInvalidStreamName,
	}, {
		name: "when there is no db error, it should write the message",
		msg:  mockMessages[0],
	}, {
		name:       "when it is asked to cancel, it does",
		msg:        mockMessages[0],
		callCancel: true,
		dbError:    errors.New("this shouldn't be returned, because we're cancelling"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			logrusLogger := logrus.New()
			repo := NewPostgresRepository(db, logrusLogger)
			ctx, cancel := context.WithCancel(context.Background())

			if test.msg != nil {
				expectedExec := mockDb.
					ExpectExec("SELECT write_message\\(\\$1, \\$2, \\$3, \\$4, \\$5\\)").
					WithArgs(
						test.msg.ID,
						test.msg.StreamName,
						test.msg.MessageType,
						test.msg.Data,
						test.msg.Metadata,
					).
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
			err := repo.WriteMessage(ctx, test.msg)

			assert.Equal(test.expectedErr, err)
		})
	}
}

func TestPostgresRepoWriteMessageWithExpectedPosition(t *testing.T) {
	tests := []struct {
		name        string
		msg         *repository.MessageEnvelope
		dbError     error
		expectedErr error
		position    int64
		callCancel  bool
	}{{
		name:        "when there is a db error, return it",
		msg:         mockMessages[0],
		dbError:     errors.New("bad things with db happened"),
		expectedErr: errors.New("bad things with db happened"),
		position:    1,
	}, {
		name:        "when there is a nil message, an error is returned",
		expectedErr: repository.ErrNilMessage,
		position:    1,
	}, {
		name:        "when the message has no ID, an error is returned",
		msg:         mockMessageNoID,
		expectedErr: repository.ErrMessageNoID,
		position:    1,
	}, {
		name:        "when the message has no stream name, an error is returned",
		msg:         mockMessageNoStream,
		expectedErr: repository.ErrInvalidStreamName,
		position:    1,
	}, {
		name:     "when the position is at 0, no error is returned",
		msg:      mockMessages[0],
		position: 0,
	}, {
		name:     "when the position is at -1, no error is returned",
		msg:      mockMessages[0],
		position: -1,
	}, {
		name:        "when the position is below -1, an error is returned",
		msg:         mockMessages[0],
		expectedErr: repository.ErrInvalidPosition,
		position:    -2,
	}, {
		name:     "when there is no db error, it should write the message",
		msg:      mockMessages[0],
		position: 1,
	}, {
		name:       "when it is asked to cancel, it does",
		msg:        mockMessages[0],
		position:   0,
		callCancel: true,
		dbError:    errors.New("this shouldn't be returned, because we're cancelling"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			db, mockDb, _ := sqlmock.New()
			logrusLogger := logrus.New()
			repo := NewPostgresRepository(db, logrusLogger)
			ctx, cancel := context.WithCancel(context.Background())

			if test.msg != nil {
				expectedExec := mockDb.
					ExpectExec("SELECT write_message\\(\\$1, \\$2, \\$3, \\$4, \\$5, \\$6\\)").
					WithArgs(test.msg.ID,
						test.msg.StreamName,
						test.msg.MessageType,
						test.msg.Data,
						test.msg.Metadata,
						test.position,
					).
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
			err := repo.WriteMessageWithExpectedPosition(ctx, test.msg, test.position)

			assert.Equal(test.expectedErr, err)
		})
	}
}
