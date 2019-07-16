package inmem_repository

import (
	"context"
	"errors"
	"fmt"

	. "github.com/blackhatbrigade/gomessagestore/repository"
)

type inmemrepo struct {
	msgs []MessageEnvelope
}

//NewInMemoryRepository creates a Repistory filled with messages
func NewInMemoryRepository(msgs []MessageEnvelope) Repository {
	return &inmemrepo{
		msgs: msgs,
	}
}

//WriteMessage writes a message
func (repo *inmemrepo) WriteMessage(ctx context.Context, message *MessageEnvelope) error {
	newMessage := *message // make myself a copy
	version := repo.findLastVersionForStream(newMessage.StreamName)
	newMessage.Version = version + 1
	globalPos := repo.findLastPosition()
	newMessage.GlobalPosition = globalPos + 1

	for _, msg := range repo.msgs {
		if msg.ID == message.ID {
			return errors.New("duplicate IDs are not allowed")
		}
	}
	repo.msgs = append(repo.msgs, newMessage)

	return nil
}

//WriteMessageWithExpectedPosition writes a message with a position
func (repo *inmemrepo) WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error {
	version := repo.findLastVersionForStream(message.StreamName)
	if version+1 != position {
		return errors.New(fmt.Sprintf("position incorrect. should be %d", version+1))
	}

	return repo.WriteMessage(ctx, message)
}

//GetAllMessagesInStream gets all messages in a stream
func (repo *inmemrepo) GetAllMessagesInStream(ctx context.Context, streamName string, batchSize int) ([]*MessageEnvelope, error) {
	msgs := make([]*MessageEnvelope, 0, batchSize)

	for _, msg := range repo.msgs {
		if msg.StreamName == streamName {
			newMessage := msg // make a copy so we don't have strangeness with slices of pointers
			msgs = append(msgs, &newMessage)
		}
		if len(msgs) == batchSize {
			return msgs, nil
		}
	}

	return msgs, nil
}

//GetAllMessagesInStreamSince gets all messages in a streams since position
func (repo *inmemrepo) GetAllMessagesInStreamSince(ctx context.Context, streamName string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error) {
	msgs := make([]*MessageEnvelope, 0, batchSize)

	atPos := false
	for _, msg := range repo.msgs {
		if msg.GlobalPosition >= globalPosition {
			atPos = true
		}

		if atPos {
			if msg.StreamName == streamName {
				newMessage := msg // make a copy so we don't have strangeness with slices of pointers
				msgs = append(msgs, &newMessage)
			}
			if len(msgs) == batchSize {
				return msgs, nil
			}
		}
	}

	return msgs, nil
}

//GetLastMessageInStream gets the last message in a stream
func (repo *inmemrepo) GetLastMessageInStream(ctx context.Context, streamName string) (foundMsg *MessageEnvelope, err error) {
	for _, msg := range repo.msgs {
		if msg.StreamName == streamName {
			newMsg := msg // make a copy so we don't just reassign based on the next item in the loop
			foundMsg = &newMsg
		}
	}

	return
}

//GetAllMessagesInCategory gets all messages in a category
func (repo *inmemrepo) GetAllMessagesInCategory(ctx context.Context, category string, batchSize int) ([]*MessageEnvelope, error) {
	msgs := make([]*MessageEnvelope, 0, batchSize)

	for _, msg := range repo.msgs {
		if msg.StreamCategory == category {
			newMessage := msg // make a copy so we don't just reassign based on the next item in the loop
			msgs = append(msgs, &newMessage)
		}
		if len(msgs) == batchSize {
			return msgs, nil
		}
	}

	return msgs, nil
}

//GetAllMessagesInCategorySince gets all messages in a category since a position
func (repo *inmemrepo) GetAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error) {
	msgs := make([]*MessageEnvelope, 0, batchSize)

	atPos := false
	for _, msg := range repo.msgs {
		if msg.GlobalPosition >= globalPosition {
			atPos = true
		}

		if atPos {
			if msg.StreamCategory == category {
				newMessage := msg // make a copy so we don't just reassign based on the next item in the loop
				msgs = append(msgs, &newMessage)
			}
			if len(msgs) == batchSize {
				return msgs, nil
			}
		}
	}

	return msgs, nil
}

func (repo *inmemrepo) findLastVersionForStream(stream string) int64 {
	var version int64
	version = -1
	for _, msg := range repo.msgs {
		if msg.StreamName == stream {
			version = msg.Version
		}
	}

	return version
}

func (repo *inmemrepo) findLastPosition() int64 {
	if len(repo.msgs) > 0 {
		return repo.msgs[len(repo.msgs)-1].GlobalPosition
	}

	return -1
}
