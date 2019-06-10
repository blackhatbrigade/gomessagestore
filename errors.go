package gomessagestore

import "errors"

//Errors
var (
	ErrGetMessagesCannotUseBothStreamAndCategory = errors.New("Get messages function cannot use both Stream and Category")
	ErrGetMessagesRequiresEitherStreamOrCategory = errors.New("Get messages function must have either Stream or Category")
)
