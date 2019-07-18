package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestWriteMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := getSampleCommandAsEnvelope()

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

	msgEnv := getSampleCommandAsEnvelope()
	var expectedPosition int64

	expectedPosition = 42

	mockRepo.
		EXPECT().
		WriteMessageWithExpectedPosition(ctx, msgEnv, expectedPosition)

	msgStore := NewMessageStoreFromRepository(mockRepo)
	msgStore.Write(ctx, msg, AtPosition(42))
}
