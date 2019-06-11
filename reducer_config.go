package gomessagestore

import (
	"time"
)

//reducerConfig Contains all of the information needed to use a given reducer.
type reducerConfig struct {
	Reducer  MessageReducer
	PollTime time.Duration
}
