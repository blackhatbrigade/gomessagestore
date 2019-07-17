package gomessagestore_test

import (
	"context"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	//_ "github.com/go-sql-driver/mysql"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
)

func TestNewMessageStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB, _, _ := sqlmock.New()

	msgStore := NewMessageStore(mockDB)

	if msgStore == nil {
		t.Error("Failed to create message store")
	}
}

func TestNewMessageStoreFromRepository(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msgStore := NewMessageStoreFromRepository(mockRepo)

	if msgStore == nil {
		t.Error("Failed to create message store from repository")
	}
}

func TestMessageStoreCanCreateAProjector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msgStore := NewMessageStoreFromRepository(mockRepo)

	msgStore.CreateProjector()
}

func TestNewMockMessageStoreWithMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	eventsToWrite := eventsToMessageSlice(getSampleEvents())
	commandsToWrite := commandsToMessageSlice(getSampleCommands())
	msgStore := NewMockMessageStoreWithMessages(append(
		eventsToWrite,
		commandsToWrite...,
	))
	if msgStore == nil {
		t.Error("Failed to create message store from repository")
		return
	}

	events, err := msgStore.Get(ctx, Category("test cat"))
	if err != nil {
		t.Errorf("Failure on Get(): %v", err)
	}
	for i, event := range events {
		if !reflect.DeepEqual(event, eventsToWrite[i]) {
			t.Errorf("Wrong event %d\nHave: %+v\nWant: %+v", i, event, eventsToWrite[i])
		}
	}

	commands, err := msgStore.Get(ctx, CommandStream("test cat"))
	if err != nil {
		t.Errorf("Failure on Get(): %v", err)
	}
	for i, command := range commands {
		if !reflect.DeepEqual(command, commandsToWrite[i]) {
			t.Errorf("Wrong command %d\nHave: %+v\nWant: %+v", i, command, commandsToWrite[i])
		}
	}
}
