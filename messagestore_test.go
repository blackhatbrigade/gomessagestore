package gomessagestore_test

import (
  "testing"

  _ "github.com/go-sql-driver/mysql"
  "github.com/golang/mock/gomock"
  mock_gomessagestore "github.com/blackhatbrigade/gomessagestore/mocks"
  . "github.com/blackhatbrigade/gomessagestore"
)

func TestWriteMessage(t *testing.T) {
  ctrl := gomock.NewController(t)
  defer ctrl.Finish()

  mockRepo := mock_gomessagestore.NewMockRepository(ctrl)

  msg := getSampleCommand()

  msgStore := &MessageStore{
    repo : mockRepo,
  }

  //msgStore.Write(
}
