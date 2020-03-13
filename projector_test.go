package gomessagestore_test

import (
	"context"
	"os"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
)

/*
  Table of contents
  1. TestProjectorAcceptsAReducer
  2. TestProjectorAcceptsADefaultState
  3. TestProjectorRunsWithReducers
  4. TestCreateProjectorFailsIfGivenPointerForDefaultState
  5. TestCreateProjectorFailsIfDefaultStateIsNotSet
  6. TestCreateProjectorFailsWithoutAtLeastOneReducer
*/

func TestProjectorAcceptsAReducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	mockred := new(mockReducer1)

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState("default state"),
		WithReducer(mockred),
	)

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
	}

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}
}

func TestProjectorAcceptsAReducerFunc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	mockred := func(msg Message, previousState interface{}) interface{} {
		switch state := previousState.(type) {
		case mockDataStructure:
			state.MockReducer1Called = true
			state.MockReducer1CallCount++
			return state
		}
		return nil
	}

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState("default state"),
		WithReducerFunc("some type", mockred),
	)

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
	}

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}
}

func TestProjectorAcceptsADefaultState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	defstate := mockDataStructure{}

	mockred := new(mockReducer1)

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
		WithReducer(mockred),
	)

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
	}

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}
}

func TestProjectorRunsWithReducers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	mockReducerFunc := func(msg Message, previousState interface{}) interface{} {
		switch state := previousState.(type) {
		case mockDataStructure:
			state.MockReducer2Called = true
			state.MockReducer2CallCount++
			return state
		}
		return nil
	}

	defstate := mockDataStructure{}

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
		WithReducer(new(mockReducer1)),
		WithReducerFunc("Event MessageType 2", mockReducerFunc),
	)

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
		return
	}

	mockEventEnvs := getSampleEventsAsEnvelopes()
	expectedEvents := getSampleEvents()
	ctx := context.Background()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, mockEventEnvs[0].StreamName, 1000).
		Return(mockEventEnvs, nil)

	projection, err := myprojector.Run(ctx, expectedEvents[0].StreamCategory, expectedEvents[0].EntityID)

	if err != nil {
		t.Errorf("An error has occurred with running a projector, err: %s", err)
	}

	if projection == nil {
		t.Error("projection from projector.Run() is nil")
	} else {
		switch myStruct := projection.(type) {
		case mockDataStructure:
			if !myStruct.MockReducer1Called {
				t.Error("Reducer 1 was not called")
			}
			if !myStruct.MockReducer2Called {
				t.Error("Reducer 2 was not called")
			}
			if myStruct.MockReducer1CallCount != len(expectedEvents)/2 {
				t.Errorf("Reducer 1 was not called the correct number of times:\nExpected: %d\n     Got: %d\n", len(expectedEvents)/2, myStruct.MockReducer1CallCount)
			}
			if myStruct.MockReducer2CallCount != len(expectedEvents)/2 {
				t.Errorf("Reducer 2 was not called the correct number of times:\nExpected: %d\n     Got: %d\n", len(expectedEvents)/2, myStruct.MockReducer2CallCount)
			}
		default:
			t.Errorf("Received incorrect type of state back: %T", projection)
		}
	}
}

func TestProjectorPicksUpAfterFullBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	defstate := mockDataStructure{}

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
		WithReducer(new(mockReducer1)),
		WithReducer(new(mockReducer2)),
	)

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
		return
	}

	mockEventEnvsBatch1 := getLotsOfSampleEventsAsEnvelopes(1000, 0)
	mockEventEnvsBatch2 := getLotsOfSampleEventsAsEnvelopes(500, 1000)
	expectedEvents := getLotsOfSampleEvents(1500, 0)
	ctx := context.Background()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, mockEventEnvsBatch1[0].StreamName, 1000).
		Return(mockEventEnvsBatch1, nil)

	mockRepo.
		EXPECT().
		GetAllMessagesInStreamSince(ctx, mockEventEnvsBatch1[0].StreamName, mockEventEnvsBatch1[len(mockEventEnvsBatch1)-1].Version+1, 1000).
		Return(mockEventEnvsBatch2, nil)

	projection, err := myprojector.Run(ctx, expectedEvents[0].StreamCategory, expectedEvents[0].EntityID)

	if err != nil {
		t.Errorf("An error has occurred with running a projector, err: %s", err)
	}

	if projection == nil {
		t.Error("projection from projector.Run() is nil")
	} else {
		switch myStruct := projection.(type) {
		case mockDataStructure:
			if !myStruct.MockReducer1Called {
				t.Error("Reducer 1 was not called")
			}
			if !myStruct.MockReducer2Called {
				t.Error("Reducer 2 was not called")
			}
			if myStruct.MockReducer1CallCount != len(expectedEvents)/2 {
				t.Errorf("Reducer 1 was not called the correct number of times:\nExpected: %d\n     Got: %d\n", len(expectedEvents)/2, myStruct.MockReducer1CallCount)
			}
			if myStruct.MockReducer2CallCount != len(expectedEvents)/2 {
				t.Errorf("Reducer 2 was not called the correct number of times:\nExpected: %d\n     Got: %d\n", len(expectedEvents)/2, myStruct.MockReducer2CallCount)
			}
		default:
			t.Errorf("Received incorrect type of state back: %T", projection)
		}
	}
}

func TestCreateProjectorFailsIfGivenPointerForDefaultState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	defstate := new(mockDataStructure)

	_, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
		WithReducer(new(mockReducer1)),
		WithReducer(new(mockReducer2)),
	)

	if err != ErrDefaultStateCannotBePointer {
		t.Errorf("Expected ErrDefaultStateCannotBePointer and got: %s\n", err)
	}
}

func TestCreateProjectorFailsIfDefaultStateIsNotSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	_, err := myMessageStore.CreateProjector(
		WithReducer(new(mockReducer1)),
		WithReducer(new(mockReducer2)),
	)

	if err != ErrDefaultStateNotSet {
		t.Errorf("Expected ErrDefaultStateNotSet and got %s\n", err)
	}

}

func TestCreateProjectorFailsWithoutAtLeastOneReducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	var logrusLogger = &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

	defstate := mockDataStructure{}

	_, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
	)

	if err != ErrProjectorNeedsAtLeastOneReducer {
		t.Errorf("Expected ErrProjectorNeedsAtLeastOneReducer and got %s\n", err)
	}
}
