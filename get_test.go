package gomessagestore_test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
)

func TestGetWithCommandStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := getSampleCommandAsEnvelope()

	logrusLogger := logrus.New()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, msgEnv.StreamName, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(ctx, CommandStream(msgEnv.StreamCategory))

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesCommand(t, msgs[0], msg)
	}
}

func TestGetWithBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := getSampleCommandAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, msgEnv.StreamName, 50).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(
		ctx,
		CommandStream(msgEnv.StreamCategory),
		BatchSize(50),
	)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesCommand(t, msgs[0], msg)
	}
}

func TestGetWithoutOptionsReturnsError(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logrusLogger := logrus.New()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	ctx := context.Background()

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	_, err := msgStore.Get(ctx)

	if err != ErrMissingGetOptions {
		t.Errorf("Expected ErrMissingGetOptions and got %v", err)
	}
}

func TestGetWithEventStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleEvent()
	ctx := context.Background()

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, msgEnv.StreamName, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(ctx, EventStream(msg.StreamCategory, msg.EntityID))

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestGetWithCategory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleEvent()
	ctx := context.Background()

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInCategory(ctx, msgEnv.StreamCategory, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(ctx, Category(msg.StreamCategory))

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestGetWithCategoryAndSince(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var globalPosition int64

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleEvent()
	ctx := context.Background()

	logrusLogger := logrus.New()
	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInCategorySince(ctx, msgEnv.StreamCategory, globalPosition, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(
		ctx,
		SincePosition(globalPosition),
		Category(msg.StreamCategory),
	)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestGetMessagesCannotUseBothStreamAndCategory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	_, err := msgStore.Get(ctx, Category(msg.StreamCategory), CommandStream(msg.StreamCategory))

	if err != ErrGetMessagesCannotUseBothStreamAndCategory {
		t.Error("Expected ErrGetMessagesCannotUseBothStreamAndCategory")
	}
}

func TestGetWithEventStreamAndSince(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logrusLogger := logrus.New()
	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleEvent()
	ctx := context.Background()
	var localPosition int64

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInStreamSince(ctx, msgEnv.StreamName, localPosition, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgs, err := msgStore.Get(
		ctx,
		SinceVersion(localPosition),
		EventStream(msg.StreamCategory, msg.EntityID),
	)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}

	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestGetWithCommandStreamAndSince(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()
	var localPosition int64

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	msgEnv := getSampleCommandAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInStreamSince(ctx, msgEnv.StreamName, localPosition, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgs, err := msgStore.Get(
		ctx,
		SinceVersion(localPosition),
		CommandStream(msg.StreamCategory),
	)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}

	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesCommand(t, msgs[0], msg)
	}
}

func TestGetMessagesRequiresEitherStreamOrCategory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var globalPosition int64

	mockRepo := mock_repository.NewMockRepository(ctrl)

	ctx := context.Background()

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	_, err := msgStore.Get(
		ctx,
		SincePosition(globalPosition),
	)

	if err != ErrGetMessagesRequiresEitherStreamOrCategory {
		t.Errorf("Expected ErrGetMessagesRequiresEitherStreamOrCategory, but got %s", err)
	}
}

func TestGetWithAlternateConverters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleOtherMessage()
	ctx := context.Background()

	msgEnv := getSampleOtherMessageAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInCategory(ctx, msgEnv.StreamCategory, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	logrusLogger := logrus.New()

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(
		ctx,
		Category(msg.StreamCategory),
		Converter(convertEnvelopeToOtherMessage),
	)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesOtherMessage(t, msgs[0], msg)
	}
}

func TestGetWithPositionSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	subscriberId := "12345"

	msg := getSampleEvent()
	ctx := context.Background()

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetLastMessageInStream(ctx, fmt.Sprintf("%s+position", subscriberId)).
		Return(msgEnv, nil)

	logrusLogger := logrus.New()

	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(
		ctx,
		PositionStream(subscriberId),
		Last(),
	)

	if err != nil {
		t.Error("An error has ocurred while getting position from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestGetWithInvalidUUIDInStreamNameSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	subscriberId := "12345"

	msg := getSampleEvent()
	msg.EntityID = NilUUID // I expect this to be empty because it didn't get parsed out
	ctx := context.Background()

	msgEnv := getSampleEventAsEnvelope()
	msgEnv.StreamName = "test cat-I'm not a uuid"

	mockRepo.
		EXPECT().
		GetLastMessageInStream(ctx, fmt.Sprintf("%s+position", subscriberId)).
		Return(msgEnv, nil)

	logrusLogger := logrus.New()
	msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	msgs, err := msgStore.Get(
		ctx,
		PositionStream(subscriberId),
		Last(),
	)

	if err != nil {
		t.Error("An error has ocurred while getting position from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		assertMessageMatchesEvent(t, msgs[0], msg)
	}
}

func TestOptionErrors(t *testing.T) {
	tests := []struct {
		name          string
		expectedError error
		opts          []GetOption
	}{{
		name:          "Last fails when stream is not set",
		expectedError: ErrGetLastRequiresStream,
		opts: []GetOption{
			Last(),
			Category("Blah"),
		},
	}, {
		name:          "Last is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			Last(),
			Last(),
			CommandStream("yayaya"),
		},
	}, {
		name:          "SincePosition is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SincePosition(5),
			SincePosition(10),
			CommandStream("yayaya"),
		},
	}, {
		name:          "SinceVersion is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SinceVersion(5),
			SinceVersion(10),
			CommandStream("yayaya"),
		},
	}, {
		name:          "SincePosition and SinceVersion are set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SincePosition(5),
			SinceVersion(10),
			CommandStream("yayaya"),
		},
	}, {
		name:          "Category is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			Category("yayaya"),
			Category("yayaya"),
		},
	}, {
		name:          "Command Stream is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			CommandStream("yayaya"),
			CommandStream("yayaya"),
		},
	}, {
		name:          "Event Stream is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			EventStream("blah", uuid1),
			EventStream("blah", uuid2),
		},
	}, {
		name:          "Position Stream is set twice",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			PositionStream("blah"),
			PositionStream("blah"),
		},
	}, {
		name:          "SincePosition and Last are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SincePosition(5),
			Last(),
			CommandStream("yayaya"),
		},
	}, {
		name:          "SinceVersion and Last are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SinceVersion(5),
			Last(),
			CommandStream("yayaya"),
		},
	}, {
		name:          "SinceVersion and Category are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SinceVersion(5),
			Category("yayaya"),
		},
	}, {
		name:          "SincePosition and EventStream are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SincePosition(5),
			EventStream("yayaya", uuid1),
		},
	}, {
		name:          "SincePosition and CommandStream are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			SincePosition(5),
			CommandStream("yayaya"),
		},
	}, {
		name:          "Command Stream and Event Stream are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			CommandStream("yayaya"),
			EventStream("blah", uuid1),
		},
	}, {
		name:          "Event Stream and Position Stream are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			EventStream("blah", uuid1),
			PositionStream("blah"),
		},
	}, {
		name:          "Command Stream and Position Stream are both set",
		expectedError: ErrInvalidOptionCombination,
		opts: []GetOption{
			CommandStream("blah"),
			PositionStream("blah"),
		},
	}, {
		name:          "Category cannot contain a hyphen",
		expectedError: ErrInvalidMessageCategory,
		opts: []GetOption{
			Category("-"),
		},
	}, {
		name:          "Command Stream cannot contain a hyphen",
		expectedError: ErrInvalidCommandStream,
		opts: []GetOption{
			CommandStream("hyphen-hyphen"),
		},
	}, {
		name:          "Event Stream cannot contain a hyphen",
		expectedError: ErrInvalidEventStream,
		opts: []GetOption{
			EventStream("hyphen-hyphen", uuid1),
		},
	}, {
		name:          "Position Stream cannot contain a hyphen",
		expectedError: ErrInvalidPositionStream,
		opts: []GetOption{
			PositionStream("hyphen-hyphen"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := mock_repository.NewMockRepository(ctrl)

			var logrusLogger = logrus.New()
			msgStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

			_, err := msgStore.Get(
				ctx,
				test.opts...,
			)

			if err != test.expectedError {
				t.Errorf("Failed to get expected error from Get\nExpected: %s\n and got: %s\n", test.expectedError, err)
			}
		})
	}
}
