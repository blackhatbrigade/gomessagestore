package inmemory_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore/repository"
	. "github.com/blackhatbrigade/gomessagestore/repository/inmemory"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/stretchr/testify/assert"
)

var streamA = []*MessageEnvelope{
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "A-123",
		StreamCategory: "A",
		MessageType:    "uh",
		Version:        5,
		GlobalPosition: 100,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "A-123",
		StreamCategory: "A",
		MessageType:    "uh",
		Version:        6,
		GlobalPosition: 104,
	},
}

var streamB = []*MessageEnvelope{
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "B-123",
		StreamCategory: "B",
		MessageType:    "uh",
		Version:        8,
		GlobalPosition: 101,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "B-123",
		StreamCategory: "B",
		MessageType:    "uh",
		Version:        9,
		GlobalPosition: 102,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "B-123",
		StreamCategory: "B",
		MessageType:    "uh",
		Version:        10,
		GlobalPosition: 107,
	},
}

var catMsgs = []*MessageEnvelope{
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "C-123",
		StreamCategory: "C",
		MessageType:    "uh",
		Version:        11,
		GlobalPosition: 103,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "C-456",
		StreamCategory: "C",
		MessageType:    "uh",
		Version:        12,
		GlobalPosition: 105,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "C-234",
		StreamCategory: "C",
		MessageType:    "uh",
		Version:        13,
		GlobalPosition: 106,
	},
	&MessageEnvelope{
		ID:             uuid.NewRandom(),
		StreamName:     "C-345",
		StreamCategory: "C",
		MessageType:    "uh",
		Version:        14,
		GlobalPosition: 108,
	},
}

var startingMessages = []MessageEnvelope{
	*streamA[0],
	*streamB[0],
	*streamB[1],
	*catMsgs[0],
	*streamA[1],
	*catMsgs[1],
	*catMsgs[2],
	*streamB[2],
	*catMsgs[3],
}

func TestInMemRepository(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	//init with a few messages
	repo := NewInMemoryRepository(startingMessages)

	//get some from stream a
	msgs, err := repo.GetAllMessagesInStream(ctx, "A-123", 1)
	assert.Equal(streamA[:1], msgs)
	assert.Nil(err)

	//get some more from stream a
	msgs, err = repo.GetAllMessagesInStreamSince(ctx, "A-123", 6, 1)
	assert.Equal(streamA[1:], msgs)
	assert.Nil(err)

	//get all from stream a
	msgs, err = repo.GetAllMessagesInStream(ctx, "A-123", 100)
	assert.Equal(streamA, msgs)
	assert.Nil(err)

	//get all from stream a the other way
	msgs, err = repo.GetAllMessagesInStreamSince(ctx, "A-123", 0, 100)
	assert.Equal(streamA, msgs)
	assert.Nil(err)

	//get last from stream b
	msg, err := repo.GetLastMessageInStream(ctx, "B-123")
	assert.Equal(streamB[2], msg)
	assert.Nil(err)

	//write an event to stream a at wrong position fails
	newID := uuid.NewRandom()
	err = repo.WriteMessageWithExpectedPosition(ctx, copyMessageWithNewID(streamA[0], newID), 0)
	assert.NotNil(err)

	//write an event to stream a at position
	err = repo.WriteMessageWithExpectedPosition(ctx, copyMessageWithNewID(streamA[0], newID), 6)
	assert.Nil(err)

	//get last from stream a
	msg, err = repo.GetLastMessageInStream(ctx, "A-123")
	assert.Equal(&MessageEnvelope{
		ID:             newID,
		StreamName:     "A-123",
		StreamCategory: "A",
		MessageType:    "uh",
		Version:        7,
		GlobalPosition: 109,
	}, msg)
	assert.Nil(err)

	//get some from category
	msgs, err = repo.GetAllMessagesInCategory(ctx, "C", 3)
	assert.Equal(catMsgs[:3], msgs)
	assert.Nil(err)

	//get some more from category
	msgs, err = repo.GetAllMessagesInCategorySince(ctx, "C", 108, 1)
	assert.Equal(catMsgs[3:], msgs)
	assert.Nil(err)

	//get all from category
	msgs, err = repo.GetAllMessagesInCategory(ctx, "C", 100)
	assert.Equal(catMsgs, msgs)
	assert.Nil(err)

	//get all from category, the other way
	msgs, err = repo.GetAllMessagesInCategorySince(ctx, "C", 0, 100)
	assert.Equal(catMsgs, msgs)
	assert.Nil(err)

	//write an event to a new stream in the same category
	newID = uuid.NewRandom()
	msg = copyMessageWithNewID(catMsgs[0], newID)
	msg.StreamName = "C-999"
	err = repo.WriteMessage(ctx, msg)
	assert.Nil(err)

	//get all from category
	msgs, err = repo.GetAllMessagesInCategory(ctx, "C", 100)
	assert.Nil(err)
	lastMsg := msgs[len(msgs)-1]
	msg.GlobalPosition = 110 // this will be what it is after we write it
	msg.Version = 0          // this will be what it is after we write it
	assert.Equal(msg, lastMsg)

	//write a command at position
	newID = uuid.NewRandom()
	cmd := &MessageEnvelope{
		ID:             newID,
		StreamName:     "R:command",
		StreamCategory: "R",
		MessageType:    "do it",
	}
	err = repo.WriteMessageWithExpectedPosition(ctx, cmd, -1)
	assert.Nil(err)

	//write it again, but at any position should fail because it is a duplicate ID
	err = repo.WriteMessage(ctx, cmd)
	assert.NotNil(err)

	//write a command at wrong position fails
	err = repo.WriteMessageWithExpectedPosition(ctx, cmd, -1)
	assert.NotNil(err)
}

func copyMessageWithNewID(msg *MessageEnvelope, id uuid.UUID) *MessageEnvelope {
	newMessage := *msg
	newMessage.ID = id

	return &newMessage
}

func TestInMemRepositoryWriteFirstMessageAtPosition(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	//init with a few messages
	repo := NewInMemoryRepository([]MessageEnvelope{})

	//write a command at position
	newID := uuid.NewRandom()
	cmd := &MessageEnvelope{
		ID:             newID,
		StreamName:     "R:command",
		StreamCategory: "R",
		MessageType:    "do it",
	}
	err := repo.WriteMessageWithExpectedPosition(ctx, cmd, -1)
	assert.Nil(err)
}
