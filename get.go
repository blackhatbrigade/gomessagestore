package gomessagestore

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

//GetOption provide optional arguments to the Get function
type GetOption func(g *getConfig)

//Stream allows for writing messages using an expected position
func CommandStream(stream string) GetOption {
	return func(g *getConfig) {
		stream := fmt.Sprintf("%s:command", stream)
		g.stream = &stream
	}
}

//Stream allows for writing messages using an expected position
func EventStream(category, entityID string) GetOption {
	return func(g *getConfig) {
		stream := fmt.Sprintf("%s-%s", category, entityID)
		g.stream = &stream
	}
}

//Get Gets one or more Messages from the message store.
func (ms *msgStore) Get(ctx context.Context, opts ...GetOption) ([]Message, error) {
	if len(opts) == 0 {
		return nil, ErrMissingGetOptions
	}

	getOptions := checkGetOptions(opts...)
	msgEnvelopes, err := ms.repo.FindAllMessagesInStream(ctx, *getOptions.stream)

	if err != nil {
		logrus.WithError(err).Error("Get: Error getting message")

		return nil, err
	}
	return msgEnvelopesToMessages(msgEnvelopes), nil
}

type getConfig struct {
	stream *string
}

func checkGetOptions(opts ...GetOption) *getConfig {
	g := &getConfig{}
	for _, option := range opts {
		option(g)
	}
	return g
}
