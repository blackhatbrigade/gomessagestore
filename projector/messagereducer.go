package projector

import (
	"github.com/blackhatbrigade/gomessagestore/repository"
)

//MessageReducer Defines the expected behaviours of a reducer that ultimately is used by the projectors.
type MessageReducer interface {
	Reduce(msg repository.Message, previousState interface{}) interface{}
}

//Errors
var ()
