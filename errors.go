package gomessagestore

import "errors"

// The following are the different error messages that can be potentially returned.
//
// The following table shows which files the specific errors are used for easier troubleshooting:
//
//	Error											| 	File Path
//	------------------------------------------------------------------------------------------------------
//	ErrInvalidOptionCombination                     |	./get.go
//	ErrSubscriberCannotUseBothStreamAndCategory     |	./subscriber_options.go
//	ErrInvalidPollTime                              |	./subscriber_options.go
//	ErrInvalidPollErrorDelay                        |	./subscriber_options.go
//	ErrInvalidBatchSize                             |	./subscriber_options.go
//	ErrInvalidMsgInterval                           |	./subscriber_options.go
//	ErrSubscriberNeedsCategoryOrStream              |	./subscriber_options.go
//	ErrSubscriberIDCannotBeEmpty                    |	./subscriber.go | ./worker_getpostion.go
//	ErrSubscriberIDCannotContainPlus                |	no uses
//	ErrSubscriberIDCannotContainHyphen              |	no uses
//	ErrPositionVersionMissing                       |	./worker_getposition.go
//	ErrSubscriberNeedsAtLeastOneMessageHandler      |	./subscriber.go
//	ErrSubscriberCannotSubscribeToMultipleStreams   |	./subscriber_options.go
//	ErrSubscriberCannotSubscribeToMultipleCategories|	./subscriber_options.go
//	ErrProjectorNeedsAtLeastOneReducer              |	./projector.go
//	ErrSubscriberMessageHandlerEqualToNil           |	./subscriber.go
//	ErrSubscriberMessageHandlersEqualToNil          |	./subscriber.go
//	ErrSubscriberNilOption                          |	./subscriber_options.go
//	ErrDefaultStateNotSet                           |	./projector.go
//	ErrDefaultStateCannotBePointer                  |	./projector.go
//	ErrGetMessagesCannotUseBothStreamAndCategory    |	./get.go
//	ErrMessageNoID                                  |	./models.go | ./worker_getposition.go
//	ErrGetMessagesRequiresEitherStreamOrCategory    |	./get.go
//	ErrGetLastRequiresStream                        |	./get.go
//	ErrIncorrectNumberOfPositionsFound              |	no uses
//	ErrInvalidHandler                               |	no uses
//	ErrIncorrectMessageInPositionStream             |	./worker_getposition
//	ErrHandlerError                                 |	no uses
//	ErrMissingMessageType                           |	./models.go | ./worker_getposition.go
//	ErrMissingMessageCategory                       |	./models.go
//	ErrInvalidMessageCategory                       |	./get.go | ./models.go
//	ErrInvalidCommandStream                         |	./get.go
//	ErrInvalidEventStream                           |	./get.go
//	ErrInvalidSubscriberID                          |	./subscriber.go
//	ErrInvalidPositionStream                        |	./get.go | ./worker_getposition.go
//	ErrMissingMessageCategoryID                     |	./models.go
//	ErrMissingMessageData                           |	./models.go
//	ErrUnserializableData                           |	./models.go | ./worker_getposition.go
//	ErrDataIsNilPointer                             |	no uses
//	ErrMissingGetOptions                            |	./get.go
var (
	ErrInvalidOptionCombination                      = errors.New("Cannot have the current combination of options for Get()")
	ErrSubscriberCannotUseBothStreamAndCategory      = errors.New("Subscriber function cannot use both Stream and Category")
	ErrInvalidPollTime                               = errors.New("Invalid Subscriber poll time provided, can not be negative or zero")
	ErrInvalidPollErrorDelay                         = errors.New("Invalid Subscriber poll error delay provided, can not be negative or zero")
	ErrInvalidBatchSize                              = errors.New("Invalid Subscriber batch size provided, can not be negative or zero")
	ErrInvalidMsgInterval                            = errors.New("MsgInterval cannot be less than 2")
	ErrSubscriberNeedsCategoryOrStream               = errors.New("Subscriber needs at least one of category or stream to be set upon creation")
	ErrSubscriberIDCannotBeEmpty                     = errors.New("Subscriber ID cannot be nil")
	ErrSubscriberIDCannotContainPlus                 = errors.New("Subscriber ID cannot contain a plus")
	ErrSubscriberIDCannotContainHyphen               = errors.New("Subscriber ID cannot contain a hyphen")
	ErrPositionVersionMissing                        = errors.New("Subscriber cannot use non-versioned positionMsg object")
	ErrSubscriberNeedsAtLeastOneMessageHandler       = errors.New("Subscriber needs at least one handler upon creation")
	ErrSubscriberCannotSubscribeToMultipleStreams    = errors.New("Subscribers can only subscribe to one stream")
	ErrSubscriberCannotSubscribeToMultipleCategories = errors.New("Subscribers can only subscribe to one category")
	ErrProjectorNeedsAtLeastOneReducer               = errors.New("Projector needs at least one reducer upon creation")
	ErrSubscriberMessageHandlerEqualToNil            = errors.New("Subscriber Message Handler cannot be equal to nil")
	ErrSubscriberMessageHandlersEqualToNil           = errors.New("Subscriber Message Handler array cannot be equal to nil")
	ErrSubscriberNilOption                           = errors.New("Options cannot include an option whose value is equal to nil")
	ErrDefaultStateNotSet                            = errors.New("Default state not set while trying to create a new projector")
	ErrDefaultStateCannotBePointer                   = errors.New("Default state cannot be a pointer when creating a projector")
	ErrGetMessagesCannotUseBothStreamAndCategory     = errors.New("Get messages function cannot use both Stream and Category")
	ErrMessageNoID                                   = errors.New("Message cannot be written without a new UUID")
	ErrGetMessagesRequiresEitherStreamOrCategory     = errors.New("Get messages function must have either Stream or Category")
	ErrGetLastRequiresStream                         = errors.New("Get Last option requires a stream")
	ErrIncorrectNumberOfPositionsFound               = errors.New("Exactly one position should be found per subscriber")
	ErrInvalidHandler                                = errors.New("Handler cannot be nil")
	ErrIncorrectMessageInPositionStream              = errors.New("Position streams can only have position messages")
	ErrHandlerError                                  = errors.New("Handler failed to handle message")
	ErrMissingMessageType                            = errors.New("All messages require a type")
	ErrMissingMessageCategory                        = errors.New("All messages require a category")
	ErrInvalidMessageCategory                        = errors.New("Hyphens are not allowed in category names")
	ErrInvalidCommandStream                          = errors.New("Hyphens are not allowed in command stream name")
	ErrInvalidEventStream                            = errors.New("Hyphens are not allowed in event stream name")
	ErrInvalidSubscriberID                           = errors.New("Hyphens and plusses are not allowed in subscriber ID")
	ErrInvalidPositionStream                         = errors.New("Position stream expects to have a single plus diving the subscriber ID and the word 'position'")
	ErrMissingMessageCategoryID                      = errors.New("All messages require a category ID")
	ErrMissingMessageData                            = errors.New("Messages payload must not be nil")
	ErrUnserializableData                            = errors.New("Message data could not be encoded as json")
	ErrDataIsNilPointer                              = errors.New("Message data is a nil pointer")
	ErrMissingGetOptions                             = errors.New("Options are required for the Get command")
	ErrExpectedVersionFailed                         = errors.New("Provided version does not match the expected version")
)
