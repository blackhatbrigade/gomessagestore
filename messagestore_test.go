package gomessagestore_test

import (
	"context"
	"reflect"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/repository/mocks"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
)

func TestWriteMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := &repository.MessageEnvelope{
		MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		Stream:     "test cat:command",
		StreamType: "test cat",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       []byte(`{"Field1":"a"}`),
	}

	mockRepo.
		EXPECT().
		WriteMessage(ctx, msgEnv)

	msgStore := GetMessageStoreInterface2(mockRepo)
	msgStore.Write(ctx, msg)
}

func TestWriteAtPosition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := &repository.MessageEnvelope{
		MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		Stream:     "test cat:command",
		StreamType: "test cat",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       []byte(`{"Field1":"a"}`),
	}
	var expectedPosition int64

	expectedPosition = 42

	mockRepo.
		EXPECT().
		WriteMessageWithExpectedPosition(ctx, msgEnv, expectedPosition)

	msgStore := GetMessageStoreInterface2(mockRepo)
	msgStore.Write(ctx, msg, AtPosition(42))
}

func TestGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mock_repository.NewMockRepository(ctrl)

	msg := getSampleCommand()
	ctx := context.Background()

	msgEnv := &repository.MessageEnvelope{
		MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		Stream:     "test cat:command",
		StreamType: "test cat",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       []byte(`{"Field1":"a"}`),
	}

	mockRepo.
		EXPECT().
		FindAllMessagesInStream(ctx, msgEnv.Stream).
		Return([]*repository.MessageEnvelope{msgEnv}, nil)

	msgStore := GetMessageStoreInterface2(mockRepo)
	msgs, err := msgStore.Get(ctx)

	if err != nil {
		t.Error("An error has ocurred while getting messages from message store")
	}
	if len(msgs) != 1 {
		t.Error("Incorrect number of messages returned")
	} else {
		switch command := msgs[0].(type) {
		case *Command:
			if command.NewID != msg.NewID {
				t.Error("NewID in message does not match")
			}
			if command.Type != msg.Type {
				t.Error("Type in message does not match")
			}
			if command.Category != msg.Category {
				t.Error("Category in message does not match")
			}
			if command.CausedByID != msg.CausedByID {
				t.Error("CausedByID in message does not match")
			}
			if command.OwnerID != msg.OwnerID {
				t.Error("OwnerID in message does not match")
			}
			data := new(dummyData)
			err = Unpack(command.Data, data)
			if err != nil {
				t.Error("Couldn't unpack data from message")
			}
			if !reflect.DeepEqual(&dummyData{"a"}, data) {
				t.Error("Messages are not correct")
			}
		default:
			t.Error("Unknown type of Message")
		}
	}
}
