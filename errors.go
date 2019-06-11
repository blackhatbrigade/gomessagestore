package gomessagestore

import "errors"

//Errors
var (
	ErrGetMessagesCannotUseBothStreamAndCategory = errors.New("Get messages function cannot use both Stream and Category")
	ErrMessageNoID                               = errors.New("Message cannot be written without a new UUID")
	ErrGetMessagesRequiresEitherStreamOrCategory = errors.New("Get messages function must have either Stream or Category")

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
	ErrMissingGetOptions               = errors.New("Options are required for the Get command")
)
