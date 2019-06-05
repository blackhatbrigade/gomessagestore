package gomessagestore

import (
	"errors"
)

//Errors
var (
	ErrIncorrectNumberOfPositionsFound = errors.New("Exactly one position should be found per subscriber")
	ErrInvalidHandler                  = errors.New("Handler cannot be nil")
	ErrHandlerError                    = errors.New("Handler failed to handle message")
  ErrMissingMessageType              = errors.New("All messages require a type")
	ErrMissingMessageCategory          = errors.New("All messages require a category")
	ErrInvalidMessageCategory          = errors.New("Hyphens are not allowed in category names")
	ErrMissingMessageCategoryID        = errors.New("All messages require a category ID")
	ErrMissingMessageData              = errors.New("Messages payload must not be nil")
	ErrUnserializableData              = errors.New("Message data could not be encoded as json")
	ErrDataIsNilPointer                = errors.New("Message data is a nil pointer")
)
