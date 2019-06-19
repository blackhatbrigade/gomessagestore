package repository

import "errors"

var (
	ErrMessageNoID       = errors.New("Message cannot be written without a new UUID")
	ErrNegativeBatchSize = errors.New("Batch size cannot be negative")
)
