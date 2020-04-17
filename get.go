package gomessagestore

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

type getOpts struct {
	stream        *string            // when set, only messages from the specified stream are retrieved
	category      *string            // when set, only messages from the specified category are retrieved
	sincePosition bool               // when set to true, only messages that occured after the specified position (since) for the category are retrieved; invalid for use with streams
	sinceVersion  bool               // when set to true, only messages that occured since teh specified version (since) for the stream are retrieved; invalid for use with categories
	since         *int64             // the position or version after which messages will be retrieved
	converters    []MessageConverter // convert non-command/event messages
	batchsize     int                // the number of messages to retrieve each round
	last          bool               // when set to true, retrieves the last message in the specified stream; invalid if stream is unspecified or since is not nil
}

// GetOption provide optional arguments to the Get function
// Invalid combinations:
// EventStream() and/or CommandStream() are called more than once
// EventStream()/CommandStream() and Category() are both called
// EventStream()/CommandStream() and Category() are both not called
// Last() is called and EventStream()/CommandStream is not called
// Last() and SincePosition()/SinceVersion() are both called
// SincePosition() and eventStream()/CommandStream() are both called
// SinceVersion() and eventStream()/CommandStream() are both called
type GetOption func(g *getOpts) error

// checkGetOptions returns the supplied options
func checkGetOptions(opts ...GetOption) (*getOpts, error) {
	g := &getOpts{batchsize: 1000} // sets batchsize to a default of 1000 if it is not set in the supplied options
	for _, option := range opts {
		if err := option(g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// Get retrieves messages from the message store that meet the criteria specified in GetOption.
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

	// Uses getOptions to determine what call to issue to retrieve correct messages
	msgEnvelopes, err := ms.callCorrectRepositoryGetFunction(ctx, getOptions)

	if err != nil {
		logrus.WithError(err).Error("Get: Error getting message")

		return nil, err
	}

	return MsgEnvelopesToMessages(msgEnvelopes, getOptions.converters...), nil
}

// Ensure that only proper combinations of getOpts are provided.
// See getOpts for more info regarding these checks
func validateGetParams(getOptions *getOpts) error {
	if getOptions.stream != nil && getOptions.category != nil {
		return ErrGetMessagesCannotUseBothStreamAndCategory
	} else if getOptions.stream == nil && getOptions.category == nil {
		return ErrGetMessagesRequiresEitherStreamOrCategory
	}
	if getOptions.last && getOptions.stream == nil {
		return ErrGetLastRequiresStream
	}
	if getOptions.last && getOptions.since != nil {
		return ErrInvalidOptionCombination
	}
	if getOptions.stream != nil && getOptions.sincePosition {
		return ErrInvalidOptionCombination // need to use SinceVersion with Streams
	}
	if getOptions.category != nil && getOptions.sinceVersion {
		return ErrInvalidOptionCombination // need to use SincePosition with Categories
	}

	return nil
}

// callCorrectRepositoryGetFunction uses the getOptions to determine which function should be called to retrieve the correct messages.
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

// GenericStream allows for getting events in a specific stream
func GenericStream(stream string) GetOption {
	return func(g *getOpts) error {
		if g.stream != nil {
			return ErrInvalidOptionCombination
		}
		g.stream = &stream
		return nil
	}
}

// EventStream allows for getting events in a specific stream
func EventStream(category string, entityID uuid.UUID) GetOption {
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

// Category allows for getting messages by category
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

// CommandCategory allows for getting messages by category
func CommandCategory(category string) GetOption {
	return func(g *getOpts) error {
		if g.category != nil {
			return ErrInvalidOptionCombination
		}
		if strings.Contains(category, "-") {
			return ErrInvalidMessageCategory
		}
		category := fmt.Sprintf("%s:command", category)
		g.category = &category
		return nil
	}
}

// PositionStream allows for getting messages by position subscriber
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

// Last allows for getting only the most recent message (still returns an array)
func Last() GetOption {
	return func(g *getOpts) error {
		if g.last {
			return ErrInvalidOptionCombination
		}
		g.last = true
		return nil
	}
}

// SincePosition allows for getting only more recent messages
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
