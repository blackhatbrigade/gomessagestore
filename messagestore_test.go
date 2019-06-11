package gomessagestore_test

import (
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
