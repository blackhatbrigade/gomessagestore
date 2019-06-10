package gomessagestore

import "errors"

var (
	ErrGetMessagesCannotUseBothStreamAndCategory = errors.New("Get messages function cannot use both Stream and Category")
)
