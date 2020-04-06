package repository

//Errors expected to be encountered by any repository
const (
	ErrMessageNoID               = Error("Message cannot be written without a new UUID")
	ErrNegativeBatchSize         = Error("Batch size cannot be negative")
	ErrInvalidSubscriberID       = Error("Subscriber ID cannot be blank")
	ErrInvalidStreamName         = Error("Stream Name cannot be blank")
	ErrBlankCategory             = Error("Category cannot be blank")
	ErrInvalidCategory           = Error("Category cannot contain a hyphen")
	ErrInvalidSubscriberPosition = Error("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                = Error("Message cannot be nil")
	ErrInvalidPosition           = Error("position must be greater than equal to -1")
)

// allows the creation of constant errors
type Error string

func (e Error) Error() string {
	return string(e)
}
