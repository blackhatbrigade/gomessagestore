package projector_test

import (
  "testing"

	"github.com/golang/mock/gomock"
  "github.com/blackhatbrigade/gomessagestore/projector"
  mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
)

func TestCanRetrieveProjector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

  projector.CreateProjector(mockRepo)
}
