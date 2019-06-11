package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

type mockDataStructure struct {
	MockReducer1Called bool
	MockReducer2Called bool
}

type mockReducer1 struct {
	PreviousState   interface{}
	ReceivedMessage Message
}

func (red *mockReducer1) Reduce(msg Message, previousState interface{}) interface{} {
	switch state := previousState.(type) {
	case mockDataStructure:
		state.MockReducer1Called = true
		return state
	}
	return nil
}

func (red *mockReducer1) Type() string {
	return "Event Type 1"
}

type mockReducer2 struct {
	PreviousState   interface{}
	ReceivedMessage Message
}

func (red *mockReducer2) Reduce(msg Message, previousState interface{}) interface{} {
	switch state := previousState.(type) {
	case mockDataStructure:
		state.MockReducer2Called = true
		return state
	}
	return nil
}

func (red *mockReducer2) Type() string {
	return "Event Type 2"
}

func TestProjectorAcceptsAReducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	myMessageStore := NewMessageStoreFromRepository(mockRepo)

	mockred := new(mockReducer1)

	myprojector, err := myMessageStore.CreateProjector(WithReducer(mockred))

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

	defstate := new(mockDataStructure)

	myprojector, err := myMessageStore.CreateProjector(DefaultState(defstate))

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

	defstate := new(mockDataStructure)

	myprojector, err := myMessageStore.CreateProjector(
		DefaultState(defstate),
		WithReducer(new(mockReducer1)),
		WithReducer(new(mockReducer2)),
	)

	if myprojector == nil {
		t.Errorf("Failed to create projector: %s", myprojector)
	}

	if err != nil {
		t.Errorf("Error creating projector: %s", err)
	}

	mockEventEnvs := getSampleEventsAsEnvelopes()
	expectedEvents := getSampleEvents()
	ctx := context.Background()

	mockRepo.
		EXPECT().
		GetAllMessagesInStream(ctx, mockEventEnvs[0].Stream).
		Return(mockEventEnvs, nil)

	runResults, err := myprojector.Run(ctx, expectedEvents[0].Category, expectedEvents[0].CategoryID)

	if err != nil {
		t.Errorf("An error has occurred with running a projector, err: %s", err)
	}

	if runResults == nil {
		t.Error("runResults from projector.Run() is nil")
	} else {
		switch myStruct := runResults.(type) {
		case mockDataStructure:
			if !myStruct.MockReducer1Called {
				t.Error("Reducer 1 was not called")
			}
			if !myStruct.MockReducer2Called {
				t.Error("Reducer 2 was not called")
			}
		default:
			t.Errorf("Received incorrect type of state back: %T", runResults)
		}
	}
}

// TODO: test that default state can't be a pointer
// TODO: test that default state is set
// TODO: test that projector has at least one reducer
