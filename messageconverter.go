package gomessagestore

import (
	"errors"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

// MessageConverter is a function that takes in a MessageEnvelope and returns a Message; can be used to create custom messages
type MessageConverter func(*repository.MessageEnvelope) (Message, error)

// MsgEnvelopesToMessages converts envelopes to any number of different structs that impliment the Message interface
func MsgEnvelopesToMessages(msgEnvelopes []*repository.MessageEnvelope, converters ...MessageConverter) []Message {
	myConverters := append(converters, defaultConverters()...)

	messages := make([]Message, 0, len(msgEnvelopes))
	for _, messageEnvelope := range msgEnvelopes {
		if messageEnvelope == nil {
			logrus.Error("Found a nil in the message envelope slice, can't transform to a message")
			continue
		}

		for _, converter := range myConverters {
			message, err := converter(messageEnvelope)
			if message != nil && err == nil {
				messages = append(messages, message)
				break // only one successful conversion per envelope
			}

		}
	}

	return messages
}

// convertEnvelopeToCommand strips out data from a MessageEnvelope to form a Message of type command
func convertEnvelopeToCommand(messageEnvelope *repository.MessageEnvelope) (Message, error) {

	streamStringParts := strings.SplitN(messageEnvelope.StreamName, "-", 2)
	if strings.HasSuffix(streamStringParts[0], ":command") {
		cmd := NewCommand(
			messageEnvelope.ID,
			messageEnvelope.EntityID,
			strings.TrimSuffix(messageEnvelope.StreamName, ":command"),
			messageEnvelope.MessageType,
			messageEnvelope.Data,
			messageEnvelope.Metadata,
		)

		cmd.MessageVersion = messageEnvelope.Version
		cmd.GlobalPosition = messageEnvelope.GlobalPosition
		cmd.Time = messageEnvelope.Time
		return cmd, nil
	}
	return nil, errors.New("Failed converting Envelope to Command, moving on to next converter")
}

// convertEnvelopeToEvent strips out data from a MessageEnvelope to form a Message of type event
func convertEnvelopeToEvent(messageEnvelope *repository.MessageEnvelope) (Message, error) {
	category := ""
	var id uuid.UUID
	cats := strings.SplitN(messageEnvelope.StreamName, "-", 2)
	if len(cats) > 0 {
		category = cats[0]
		if len(cats) == 2 {
			id, _ = uuid.Parse(cats[1]) // errors on parsing just leave entityID blank
		}
	}
	evt := NewEvent(
		messageEnvelope.ID,
		id,
		category,
		messageEnvelope.MessageType,
		messageEnvelope.Data,
		messageEnvelope.Metadata,
	)

	evt.MessageVersion = messageEnvelope.Version
	evt.GlobalPosition = messageEnvelope.GlobalPosition
	evt.Time = messageEnvelope.Time
	return evt, nil
}

func defaultConverters() []MessageConverter {
	return []MessageConverter{
		convertEnvelopeToCommand,
		convertEnvelopeToEvent, // always run this one last, as it always passes
	}
}
