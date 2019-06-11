package gomessagestore_test

import (
	"testing"

	"github.com/blackhatbrigade/gomessagestore"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

type mockReducer struct {
	PreviousState   interface{}
	ReceivedMessage gomessagestore.Message
}

func (red *mockReducer) Reduce(msg gomessagestore.Message, previousState interface{}) interface{} {
	return nil
}

func TestProjectorAcceptsAReducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	myMessageStore := gomessagestore.GetMessageStoreInterface2(mockRepo)

	myprojector := myMessageStore.CreateProjector()

	mockred := new(mockReducer)

	myprojector.RegisterReducer(mockred)
}
