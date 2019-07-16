package gomessagestore

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore MessageReducer > mocks/message_reducer.go"

//MessageReducer Defines the expected behaviours of a reducer that ultimately is used by the projectors.
type MessageReducer interface {
	Reduce(msg Message, previousState interface{}) interface{}
	Type() string
}

//MessageReducerConfig Contains all of the information needed to use a given reducer.
type MessageReducerConfig struct {
	Reducer MessageReducer
	Type    string
}
