package gomessagestore

import (
	"github.com/blackhatbrigade/gomessagestore/repository"
	"golang.org/x/net/context"
)

//ReducerOption Variadic parameter support for reducers.
type ReducerOption func(proj *projector)

//Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
	RegisterReducer(reducer MessageReducer, opts ...ReducerOption) error
	Run(ctx context.Context) (interface{}, error)
}

//projector The base supported projector struct.
type projector struct {
	repo     repository.Repository
	reducers []MessageReducerConfig
}

func createProjector(repoRef repository.Repository) Projector {
	proj := &projector{
		repo: repoRef,
	}
	return proj
}

func (proj *projector) RegisterReducer(reducer MessageReducer, opts ...ReducerOption) error {

	return nil
}

func (proj *projector) Run(ctx context.Context) (interface{}, error) {
	return nil, nil
}
