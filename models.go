package messagestore

//Command the model for writing a command to the Message Store
type Command struct {
	NewID         string
	Type          string
	Category      string
	CausedByID    string
	OwnerID        string
	Data          interface{}
}

//Event the model for writing an event to the Message Store
type Event struct {
	NewID         string
	Type          string
	CategoryID    string
	Category      string
	CausedByID    string
	OwnerID       string
	Data          interface{}
}

//MessageEnvelope the model for data read from the Message Store
type MessageEnvelope struct {
	GlobalPosition int64     `json:"global_position" db:"global_position"`
	MessageID      string    `json:"message_id" db:"message_id"`
	Type           string    `json:"type" db:"type"`
	Stream         string    `json:"stream" db:"stream"`
	StreamType     string    `json:"stream_type" db:"stream_type"`
	CausedByID     string    `json:"caused_by_id" db:"caused_by_id"`
	OwnerID        string    `json:"user_id" db:"user_id"`
	Position       int64     `json:"position" db:"position"`
	Data           []byte    `json:"data" db:"data"`
	Timestamp      time.Time `json:"timestamp" db:"timestamp"`
}
