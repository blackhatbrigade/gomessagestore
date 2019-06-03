package gomessagestore

import (
	"errors"
)

//Errors
var (
	ErrIncorrectNumberOfPositionsFound = errors.New("Exactly one position should be found per subscriber")
	ErrInvalidSubscriberID             = errors.New("Subscriber ID cannot be blank")
	ErrInvalidStreamID                 = errors.New("Stream ID cannot be blank")
	ErrInvalidSubscriberPosition       = errors.New("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                      = errors.New("Message cannot be nil")
	ErrMessageNoID                     = errors.New("Message cannot be written without a new UUID")
	ErrInvalidPosition                 = errors.New("position must be greater than equal to -1")
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
