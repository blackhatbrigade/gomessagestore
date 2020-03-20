package gomessagestore_test

import (
	"context"
	"io/ioutil"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
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

	var logrusLogger = logrus.New()
	logrusLogger.Out = ioutil.Discard
	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	myMessageStore.Write(ctx, msg)
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

	var logrusLogger = logrus.New()
	logrusLogger.Out = ioutil.Discard
	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)
	myMessageStore.Write(ctx, msg, AtPosition(42))
}

func TestAtPositionMatcher(t *testing.T) {
	atPosition := AtPosition(42)
	matcher := AtPositionMatcher{42}

	if !matcher.Matches(atPosition) {
		t.Errorf("Incorrect AtPosition")
	}
}
