package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
)

func TestGetWithCommandStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := getSampleCommandAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, msgEnv.StreamName, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	mockRepo := mock_repository.NewMockRepository(ctrl)

	ctx := context.Background()

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInCategorySince(ctx, msgEnv.StreamCategory, globalPosition, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgStore := NewMessageStoreFromRepository(mockRepo)
	_, err := msgStore.Get(ctx, Category(msg.StreamCategory), CommandStream(msg.StreamCategory))

	if err != ErrGetMessagesCannotUseBothStreamAndCategory {
		t.Error("Expected ErrGetMessagesCannotUseBothStreamAndCategory")
	}
}

func TestGetWithEventStreamAndSince(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleEvent()
	ctx := context.Background()
	var localPosition int64

	msgStore := NewMessageStoreFromRepository(mockRepo)

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

	msgStore := NewMessageStoreFromRepository(mockRepo)

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

	msgStore := NewMessageStoreFromRepository(mockRepo)
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

	msgEnv := getSampleEventAsEnvelope()

	mockRepo.
		EXPECT().
		GetAllMessagesInCategory(ctx, msgEnv.StreamCategory, 1000).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := NewMessageStoreFromRepository(mockRepo)
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
