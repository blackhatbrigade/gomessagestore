package gomessagestore

import (
	"github.com/blackhatbrigade/gomessagestore/uuid"
)

// NilUUID is a helper for tests
var NilUUID = uuid.Nil

// NewID creates a new UUID.
func NewID() uuid.UUID {
	return uuid.NewRandom()
}
