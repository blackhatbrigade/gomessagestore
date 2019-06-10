package message

import (
	"time"
)

//MessageEnvelope the model for data read from the Message Store
type MessageEnvelope struct {
	GlobalPosition int64     `json:"global_position" db:"global_position"`
	MessageID      string    `json:"message_id" db:"message_id"`
	Type           string    `json:"type" db:"type"`
	Stream         string    `json:"stream" db:"stream"`
	StreamType     string    `json:"stream_type" db:"stream_type"`
	CorrelationID  string    `json:"correlation_id" db:"correlation_id"`
	CausedByID     string    `json:"caused_by_id" db:"caused_by_id"`
	UserID         string    `json:"user_id" db:"user_id"`
	OwnerID        string    `json:"owner_id" db:"owner_id"`
	Position       int64     `json:"position" db:"position"`
	Data           []byte    `json:"data" db:"data"`
	Timestamp      time.Time `json:"timestamp" db:"timestamp"`
}
