package gomessagestore

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

type getOpts struct {
	stream        *string
	category      *string
	sincePosition bool
	sinceVersion  bool
	since         *int64
	converters    []MessageConverter
	batchsize     int
	last          bool
}

//GetOption provide optional arguments to the Get function
type GetOption func(g *getOpts) error

func checkGetOptions(opts ...GetOption) (*getOpts, error) {
	g := &getOpts{batchsize: 1000}
	for _, option := range opts {
		if err := option(g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

//Get Gets one or more Messages from the message store.
func (ms *msgStore) Get(ctx context.Context, opts ...GetOption) ([]Message, error) {

	if len(opts) == 0 {
		return nil, ErrMissingGetOptions
	}

	getOptions, err := checkGetOptions(opts...)
	if err != nil {
		return nil, err
	}

	// param validation
	if err := validateGetParams(getOptions); err != nil {
		return nil, err
	}

	// choose a path
	msgEnvelopes, err := ms.callCorrectRepositoryGetFunction(ctx, getOptions)

	if err != nil {
		logrus.WithError(err).Error("Get: Error getting message")

		return nil, err
	}

	return MsgEnvelopesToMessages(msgEnvelopes, getOptions.converters...), nil
}

func validateGetParams(getOptions *getOpts) error {
	// one, and only one of stream or category
	if getOptions.stream != nil && getOptions.category != nil {
		return ErrGetMessagesCannotUseBothStreamAndCategory
	} else if getOptions.stream == nil && getOptions.category == nil {
		return ErrGetMessagesRequiresEitherStreamOrCategory
	}

	// potentially bad combinations
	if getOptions.last && getOptions.stream == nil {
		return ErrGetLastRequiresStream
	}
	if getOptions.last && getOptions.since != nil {
		return ErrInvalidOptionCombination
	}

	return nil
}

func (ms *msgStore) callCorrectRepositoryGetFunction(ctx context.Context, getOptions *getOpts) (msgEnvelopes []*repository.MessageEnvelope, err error) {
	if getOptions.since != nil {
		if getOptions.stream != nil {
			msgEnvelopes, err = ms.repo.GetAllMessagesInStreamSince(ctx, *getOptions.stream, *getOptions.since, getOptions.batchsize)
		} else {
			msgEnvelopes, err = ms.repo.GetAllMessagesInCategorySince(ctx, *getOptions.category, *getOptions.since, getOptions.batchsize)
		}
	} else {
		if getOptions.last {
			var msg *repository.MessageEnvelope
			msg, err = ms.repo.GetLastMessageInStream(ctx, *getOptions.stream)
			if msg != nil {
				msgEnvelopes = []*repository.MessageEnvelope{msg}
			}
		} else {

			if getOptions.stream != nil {
				msgEnvelopes, err = ms.repo.GetAllMessagesInStream(ctx, *getOptions.stream, getOptions.batchsize)
			}

			if getOptions.category != nil {
				msgEnvelopes, err = ms.repo.GetAllMessagesInCategory(ctx, *getOptions.category, getOptions.batchsize)

			}
		}
	}

	return
}

//CommandStream allows for writing messages using an expected position
func CommandStream(category string) GetOption {
	return func(g *getOpts) error {
		if g.stream != nil {
			return ErrInvalidOptionCombination
		}
		if strings.Contains(category, "-") {
			return ErrInvalidCommandStream
		}
		stream := fmt.Sprintf("%s:command", category)
		g.stream = &stream
		return nil
	}
}

//EventStream allows for getting events in a specific stream
func EventStream(category, entityID string) GetOption {
	return func(g *getOpts) error {
		if g.stream != nil {
			return ErrInvalidOptionCombination
		}
		if strings.Contains(category, "-") {
			return ErrInvalidEventStream
		}
		stream := fmt.Sprintf("%s-%s", category, entityID)
		g.stream = &stream
		return nil
	}
}

//Category allows for getting messages by category
func Category(category string) GetOption {
	return func(g *getOpts) error {
		if g.category != nil {
			return ErrInvalidOptionCombination
		}
		if strings.Contains(category, "-") {
			return ErrInvalidMessageCategory
		}
		g.category = &category
		return nil
	}
}

//Position allows for getting messages by position subscriber
func PositionStream(subscriberID string) GetOption {
	return func(g *getOpts) error {
		if g.stream != nil {
			return ErrInvalidOptionCombination
		}
		if strings.Contains(subscriberID, "-") {
			return ErrInvalidPositionStream
		}
		stream := fmt.Sprintf("%s+position", subscriberID)
		g.stream = &stream
		return nil
	}
}

//Last allows for getting only the most recent message (still returns an array)
func Last() GetOption {
	return func(g *getOpts) error {
		if g.last {
			return ErrInvalidOptionCombination
		}
		g.last = true
		return nil
	}
}

//SincePosition allows for getting only more recent messages
func SincePosition(position int64) GetOption {
	return func(g *getOpts) error {
		if g.since != nil {
			return ErrInvalidOptionCombination
		}
		g.since = &position
		g.sincePosition = true
		return nil
	}
}

//SinceVersion allows for getting only more recent messages
func SinceVersion(version int64) GetOption {
	return func(g *getOpts) error {
		if g.since != nil {
			return ErrInvalidOptionCombination
		}
		g.since = &version
		g.sinceVersion = true
		return nil
	}
}

//Converter allows for automatic converting of non-Command/Event type messages
func Converter(converter MessageConverter) GetOption {
	return func(g *getOpts) error {
		g.converters = append(g.converters, converter)
		return nil
	}
}

//BatchSize changes how many messages are returned (default 1000)
func BatchSize(batchsize int) GetOption {
	return func(g *getOpts) error {
		g.batchsize = batchsize
		return nil
	}
}
