package gomessagestore

import (
	"context"
	"reflect"

	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Projector > mocks/projector.go"

//CreateProjector creates a new Projector based on the provided ProjectorOption
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

// ProjectorOption is used for creating projectors with reducers
type ProjectorOption func(proj *projector)

// Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
	Run(ctx context.Context, category string, entityID uuid.UUID) (interface{}, error)
	RunOnStream(ctx context.Context, stream string) (interface{}, error)
	Step(msg Message, previousState interface{}) (interface{}, bool, error)
}

// projector The base projector struct.
type projector struct {
	ms           MessageStore
	reducers     []MessageReducer
	defaultState interface{}
}

// RunOnStream retrieves all messages for a given stream, and runs the projector on each message found
func (proj *projector) RunOnStream(ctx context.Context, stream string) (interface{}, error) {
	return proj.run(ctx, stream)
}

// Run retrieves all messages for a given category and entity, and runs the projector on each message found
func (proj *projector) Run(ctx context.Context, category string, entityID uuid.UUID) (interface{}, error) {
	return proj.run(ctx, category+"-"+entityID.String())
}

// run calls getMessages, for a given category and id, on the projector and runs each message through a matching reducer to derive the state, and returns the state after all messages are processed
func (proj *projector) run(ctx context.Context, stream string) (interface{}, error) {
	msgs, err := proj.getMessages(ctx, stream)

	if err != nil {
		return nil, err
	}

	state := proj.defaultState
	for _, message := range msgs {
		if newState, ok, err := proj.Step(message, state); err != nil {
			return nil, err
		} else if ok {
			state = newState
		}
	}

	return state, nil
}

// Step is ran for each message, iterating the state for the reducer mapped to that message
func (proj *projector) Step(msg Message, previousState interface{}) (interface{}, bool, error) {
	for _, reducer := range proj.reducers {
		if reducer.Type() == msg.Type() {
			if reduction, err := reducer.Reduce(msg, previousState); err == nil {
				return reduction, true, nil
			} else {
				return nil, false, err
			}
		}
	}
	return nil, false, nil
}

//WithReducer registers a ruducer with the new projector
func WithReducer(reducer MessageReducer) ProjectorOption {
	return func(proj *projector) {
		proj.reducers = append(proj.reducers, reducer)
	}
}

//WithReducerFunc registers a message type and a ruducer function with the new projector
func WithReducerFunc(msgType string, reducerFunc MessageReducerFunc) ProjectorOption {
	return func(proj *projector) {
		reducer := &genericReducer{msgType, reducerFunc}
		proj.reducers = append(proj.reducers, reducer)
	}
}

//DefaultState registers a default state for use with a projector
func DefaultState(defaultState interface{}) ProjectorOption {
	return func(proj *projector) {
		proj.defaultState = defaultState
	}
}

// getMessages retrieves messages from the message store
func (proj *projector) getMessages(ctx context.Context, stream string) ([]Message, error) {
	batchsize := 1000
	msgs, err := proj.ms.Get(ctx,
		GenericStream(stream),
		BatchSize(batchsize),
	)
	if err != nil {
		return nil, err
	}

	if len(msgs) == batchsize {
		allMsgs := make([]Message, 0, batchsize*2)
		allMsgs = append(allMsgs, msgs...)
		for len(msgs) == batchsize {
			msgs, err = proj.ms.Get(ctx,
				GenericStream(stream),
				BatchSize(batchsize),
				SinceVersion(msgs[batchsize-1].Version()+1), // Since grabs an inclusive list, so grab 1 after the latest version
			)
			if err != nil {
				return nil, err
			}

			allMsgs = append(allMsgs, msgs...)
		}

		return allMsgs, nil
	}

	return msgs, nil
}
