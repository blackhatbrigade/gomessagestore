package projector

//reducerConfig Contains all of the information needed to use a given reducer.
type reducerConfig struct {
	Reducer  MessageReducer
	PollTime int64
}
