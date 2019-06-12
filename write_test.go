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

func TestWriteMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		StreamName:     "test cat:command",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
	}

	mockRepo.
		EXPECT().
		WriteMessage(ctx, msgEnv)

	msgStore := NewMessageStoreFromRepository(mockRepo)
	msgStore.Write(ctx, msg)
}

func TestWriteWithAtPosition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		StreamName:     "test cat:command",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
	}
	var expectedPosition int64

	expectedPosition = 42

	mockRepo.
		EXPECT().
		WriteMessageWithExpectedPosition(ctx, msgEnv, expectedPosition)

	msgStore := NewMessageStoreFromRepository(mockRepo)
	msgStore.Write(ctx, msg, AtPosition(42))
}
