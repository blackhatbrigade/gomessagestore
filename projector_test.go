package gomessagestore_test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
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

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

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

func TestProjectorAcceptsADefaultState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

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

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

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

	mockEventEnvs := getSampleEventsAsEnvelopes()
	expectedEvents := getSampleEvents()
	ctx := context.Background()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, mockEventEnvs[0].StreamName).
		Return(mockEventEnvs, nil)

	projection, err := myprojector.Run(ctx, expectedEvents[0].StreamCategory, expectedEvents[0].EntityID)

	fmt.Printf("projection: %s\n", projection)

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
		default:
			t.Errorf("Received incorrect type of state back: %T", projection)
		}
	}
}

func TestCreateProjectorFailsIfGivenPointerForDefaultState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

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

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

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

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

	defstate := mockDataStructure{}

	_, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
	)

	if err != ErrProjectorNeedsAtLeastOneReducer {
		t.Errorf("Expected ErrProjectorNeedsAtLeastOneReducer and got %s\n", err)
	}
}
