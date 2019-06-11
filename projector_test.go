package gomessagestore_test

import (
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestCanRetrieveProjector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	CreateProjector(mockRepo)
}
