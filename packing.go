package gomessagestore

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

//Unpack unpacks JSON-esque objects used in the Command and Event objects into GO objects
func Unpack(source map[string]interface{}, dest interface{}) error {
	inbetween, err := json.Marshal(source)
	if err != nil {
		return err
	}

	return json.Unmarshal(inbetween, dest)
}

//Pack packs a GO object into JSON-esque objects used in the Command and Event objects
func Pack(source interface{}) (map[string]interface{}, error) {
	dest := make(map[string]interface{})
	inbetween, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(inbetween, &dest)
	return dest, err
}

//MessageConverter allows the MsgEnvelopesToMessages to convert to structs that aren't defined in this library
// if the message isn't the correct type, returning nil or an error will cause the default converters to run
type MessageConverter func(*repository.MessageEnvelope) (Message, error)

//MsgEnvelopesToMessages converts envelopes to any number of different structs that impliment the Message interface
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

func convertEnvelopeToCommand(messageEnvelope *repository.MessageEnvelope) (Message, error) {
	if strings.HasSuffix(messageEnvelope.StreamName, ":command") {
		data := make(map[string]interface{})
		if err := json.Unmarshal(messageEnvelope.Data, &data); err != nil {
			logrus.WithError(err).Error("Can't unmarshal JSON from message envelope data")
		}
		metadata := make(map[string]interface{})
		if err := json.Unmarshal(messageEnvelope.Metadata, &metadata); err != nil {
			logrus.WithError(err).Error("Can't unmarshal JSON from message envelope metadata")
		}
		command := &Command{
			ID:             messageEnvelope.ID,
			MessageType:    messageEnvelope.MessageType,
			StreamCategory: strings.TrimSuffix(messageEnvelope.StreamName, ":command"),
			Position:       messageEnvelope.Position,
			GlobalPosition: messageEnvelope.GlobalPosition,
			Data:           data,
			Metadata:       metadata,
			Time:           messageEnvelope.Time,
		}

		return command, nil
	} else {
		return nil, errors.New("not a command, also, this isn't an error")
	}
}

func convertEnvelopeToEvent(messageEnvelope *repository.MessageEnvelope) (Message, error) {
	data := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Data, &data); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope data")
	}
	metadata := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Metadata, &metadata); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope metadata")
	}
	category, id := "", ""
	cats := strings.SplitN(messageEnvelope.StreamName, "-", 2)
	if len(cats) > 0 {
		category = cats[0]
		if len(cats) == 2 {
			id = cats[1]
		}
	}
	event := &Event{
		ID:             messageEnvelope.ID,
		Position:       messageEnvelope.Position,
		GlobalPosition: messageEnvelope.GlobalPosition,
		MessageType:    messageEnvelope.MessageType,
		StreamCategory: category,
		EntityID:       id,
		Data:           data,
		Metadata:       metadata,
		Time:           messageEnvelope.Time,
	}

	return event, nil
}

func defaultConverters() []MessageConverter {
	return []MessageConverter{
		convertEnvelopeToCommand,
		convertEnvelopeToEvent, // always run this one last, as it always passes
	}
}
