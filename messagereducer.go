package gomessagestore

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
