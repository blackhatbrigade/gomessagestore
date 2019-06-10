package gomessagestore

import (
	"context"
	"fmt"

	"github.com/blackhatbrigade/gomessagestore/message"
	"github.com/sirupsen/logrus"
)

type getter struct {
	stream *string
}

//GetOption provide optional arguments to the Get function
type GetOption func(g *getter)

func checkGetOptions(opts ...GetOption) *getter {
	g := &getter{}
	for _, option := range opts {
		option(g)
	}
	return g
}

//Get Gets one or more Messages from the message store.
func (ms *msgStore) Get(ctx context.Context, opts ...GetOption) ([]message.Message, error) {

	if len(opts) == 0 {
		return nil, message.ErrMissingGetOptions
	}

	getOptions := checkGetOptions(opts...)
	msgEnvelopes, err := ms.repo.FindAllMessagesInStream(ctx, *getOptions.stream)

	if err != nil {
		logrus.WithError(err).Error("Get: Error getting message")

		return nil, err
	}
	return message.MsgEnvelopesToMessages(msgEnvelopes), nil
}

//Stream allows for writing messages using an expected position
func CommandStream(stream string) GetOption {
	return func(g *getter) {
		stream := fmt.Sprintf("%s:command", stream)
		g.stream = &stream
	}
}

//Stream allows for writing messages using an expected position
func EventStream(category, entityID string) GetOption {
	return func(g *getter) {
		stream := fmt.Sprintf("%s-%s", category, entityID)
		g.stream = &stream
	}
}
