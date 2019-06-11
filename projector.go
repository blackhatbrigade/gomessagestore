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
	reducers []reducerConfig
}

func CreateProjector(repoRef repository.Repository) Projector {
	proj := &projector{
		repo: repoRef,
	}
	return proj
}

//Errors
var ()
