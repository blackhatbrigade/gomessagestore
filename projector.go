package gomessagestore

import (
	"context"
	"reflect"
)

//CreateProjector creates a projector for use with MessageReducers to get projections
func (ms *msgStore) CreateProjector(opts ...ProjectorOption) (Projector, error) {
	projector := &projector{
		ms: ms,
	}

	for _, option := range opts {
		option(projector)
	}

	//make sure defaultState is not a pointer
	if reflect.ValueOf(projector.defaultState).Kind() == reflect.Ptr {
		return nil, ErrDefaultStateCannotBePointer
	}

	if len(projector.reducers) < 1 {
		return nil, ErrProjectorNeedsAtLeastOneReducer
	}

	if projector.defaultState == nil {
		return nil, ErrDefaultStateNotSet
	}

	return projector, nil
}

//ReducerOption Variadic parameter support for reducers.
type ProjectorOption func(proj *projector)

//Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
	Run(ctx context.Context, category string, entityID string) (interface{}, error)
}

//projector The base supported projector struct.
type projector struct {
	ms           MessageStore
	reducers     []MessageReducer
	defaultState interface{}
}

func (proj *projector) Run(ctx context.Context, category string, entityID string) (interface{}, error) {
	msgs, err := proj.ms.Get(ctx,
		EventStream(category, entityID),
	)

	if err != nil {
		return nil, err
	}

	state := proj.defaultState
	for _, message := range msgs {
		for _, reducer := range proj.reducers {
			if reducer.Type() == message.Type() {
				state = reducer.Reduce(message, state)
			}
		}
	}

	return state, nil
}

//WithReducer registers a ruducer with the new projector
func WithReducer(reducer MessageReducer) ProjectorOption {
	return func(proj *projector) {
		proj.reducers = append(proj.reducers, reducer)
	}
}

//DefaultState registers a default state for use with a projector
func DefaultState(defaultState interface{}) ProjectorOption {
	return func(proj *projector) {
		proj.defaultState = defaultState
	}
}
