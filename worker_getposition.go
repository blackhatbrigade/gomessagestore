package gomessagestore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

// GetPosition retrieves the current position that messages should be retrieved from; first process of the polling loop
func (sw *subscriptionWorker) GetPosition(ctx context.Context) (int64, error) {
	log := logrus.
		WithFields(logrus.Fields{
			"SubscriberID": sw.subscriberID,
		})

	msgs, _ := sw.ms.Get(
		ctx,
		PositionStream(sw.subscriberID),
		Converter(convertEnvelopeToPositionMessage),
		Last(),
	)
	if len(msgs) < 1 {
		log.Debug("no messages found for subscriber, using default")
		return 0, nil
	}

	switch pos := msgs[0].(type) {
	case *positionMessage:
		return pos.MyPosition, nil
	default:
		log.
			WithError(ErrIncorrectMessageInPositionStream).
			Error("incorrect message type in position stream")
		return 0, nil
	}
}

// convertEnvelopeToPositionMessage takes a messageEnvelope and converts it into a PositionMessage that is used to keep track of position changes
func convertEnvelopeToPositionMessage(messageEnvelope *repository.MessageEnvelope) (Message, error) {
	data := positionData{}
	if err := json.Unmarshal(messageEnvelope.Data, &data); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope data")
		return nil, err
	}

	halves := strings.Split(messageEnvelope.StreamName, "+")
	if len(halves) != 2 || halves[1] != "position" {
		return nil, ErrInvalidPositionStream
	}

	positionMsg := &positionMessage{
		ID:             messageEnvelope.ID,
		MyPosition:     data.Position,
		MessageVersion: messageEnvelope.Version,
		SubscriberID:   halves[0],
	}
	return positionMsg, nil
}

// positionMessage is a message type used to keep track of changes in position so that messages are not read multiple times or skipped
type positionMessage struct {
	ID             uuid.UUID
	MyPosition     int64
	SubscriberID   string
	MessageVersion int64
	GlobalPosition int64
}

type positionData struct {
	Position int64 `json:"position"`
}

func (posMsg *positionMessage) Type() string {
	return "PositionCommitted"
}

func (posMsg *positionMessage) Version() int64 {
	return posMsg.MessageVersion
}

func (posMsg *positionMessage) Position() int64 {
	return posMsg.GlobalPosition
}

func (posMsg *positionMessage) ToEnvelope() (*repository.MessageEnvelope, error) {
	messageType := posMsg.Type()

	if messageType == "" {
		return nil, ErrMissingMessageType
	}

	if posMsg.ID == NilUUID {
		return nil, ErrMessageNoID
	}

	if posMsg.SubscriberID == "" {
		return nil, ErrSubscriberIDCannotBeEmpty
	}

	if posMsg.MessageVersion < 0 {
		return nil, ErrPositionVersionMissing
	}

	posData := positionData{posMsg.MyPosition}

	data, err := json.Marshal(posData)
	if err != nil {
		return nil, ErrUnserializableData
	}

	msgEnv := &repository.MessageEnvelope{
		ID:             posMsg.ID,
		MessageType:    messageType,
		StreamName:     fmt.Sprintf("%s+position", posMsg.SubscriberID),
		Data:           data,
		Version:        posMsg.MessageVersion,
		GlobalPosition: posMsg.GlobalPosition,
	}

	return msgEnv, nil
}
