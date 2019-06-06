package projector_test

import (
	"testing"

	"github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/projector"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

type mockReducer struct {
	PreviousState   interface{}
	ReceivedMessage gomessagestore.Message
}

func (red *mockReducer) Reduce(message gomessagestore.Message, previousState interface{}) {
}

func TestProjectorAcceptsAReducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	myprojector := projector.CreateProjector(mockRepo)

	mockred := new(mockReducer)

	myprojector.RegisterReducer(mockred)
}
