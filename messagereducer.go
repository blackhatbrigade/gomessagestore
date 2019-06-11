package gomessagestore

//MessageReducer Defines the expected behaviours of a reducer that ultimately is used by the projectors.
type MessageReducer interface {
	Reduce(msg Message, previousState interface{}) interface{}
}
