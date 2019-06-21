package gomessagestore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

//GetPosition starts phase one of the polling loop
func (sw *subscriptionWorker) GetPosition(ctx context.Context) (int64, error) {
	sub := sw.sub
	log := logrus.
		WithFields(logrus.Fields{
			"SubscriberID": sub.subscriberID,
		})

	msgs, _ := sw.ms.Get(
		ctx,
		PositionStream(sub.subscriberID),
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

type positionMessage struct {
	ID             string
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

	if posMsg.ID == "" {
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
		StreamName:     fmt.Sprintf("%s+position", posMsg.ID),
		Data:           data,
		Version:        posMsg.MessageVersion,
		GlobalPosition: posMsg.GlobalPosition,
	}

	return msgEnv, nil
}
