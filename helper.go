package gomessagestore

import (
	"encoding/json"
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

func msgEnvelopesToMessages(msgEnvelopes []*repository.MessageEnvelope) []Message {
	messages := make([]Message, 0, len(msgEnvelopes))
	for _, messageEnvelope := range msgEnvelopes {
		if messageEnvelope == nil {
			logrus.Error("Found a nil in the message envelope slice, can't transform to a message")
			continue
		}
		data := make(map[string]interface{})
		err := json.Unmarshal(messageEnvelope.Data, &data)
		if err != nil {
			logrus.WithError(err).Error("Can't unmarshal JSON from message envelope")
			continue
		}
		if strings.HasSuffix(messageEnvelope.Stream, ":command") {
			command := &Command{
				NewID:      messageEnvelope.MessageID,
				Type:       messageEnvelope.Type,
				Category:   strings.TrimSuffix(messageEnvelope.Stream, ":command"),
				CausedByID: messageEnvelope.CausedByID,
				OwnerID:    messageEnvelope.OwnerID,
				Data:       data,
			}
			messages = append(messages, command)
		} else {
			category, id := "", ""
			cats := strings.SplitN(messageEnvelope.Stream, "-", 2)
			if len(cats) > 0 {
				category = cats[0]
				if len(cats) == 2 {
					id = cats[1]
				}
			}
			event := &Event{
				NewID:      messageEnvelope.MessageID,
				Type:       messageEnvelope.Type,
				Category:   category,
				CategoryID: id,
				CausedByID: messageEnvelope.CausedByID,
				OwnerID:    messageEnvelope.OwnerID,
				Data:       data,
			}
			messages = append(messages, event)
		}
	}

	return messages
}
