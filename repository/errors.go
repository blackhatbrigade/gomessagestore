package repository

import "errors"

var (
	ErrMessageNoID = errors.New("Message cannot be written without a new UUID")
)
