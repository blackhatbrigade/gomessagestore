package gomessagestore

import (
	"testing"

	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
)

func TestMessageStoreCanCreateAProjector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msgStore := GetMessageStoreInterface2(mockRepo)

	msgStore.CreateProjector()
}
