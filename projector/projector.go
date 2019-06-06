package projector

import (
	"github.com/blackhatbrigade/gomessagestore/repository"
	"golang.org/x/net/context"
)

//ReducerOption Variadic parameter support for reducers.
type ReducerOption func(proj *projector)

//Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
	RegisterReducer(reducer MessageReducer, opts ...ReducerOption) error
	Run(ctx context.Context) error
}

//projector The base supported projector struct.
type projector struct {
	repo     repository.Repository
	reducers []reducerConfig
}

//CreateProjector Creates a default projector struct that conforms to the interface.
func CreateProjector(repos repository.Repository) (proj Projector) {
	proj = &projector{
		repo:     repos,
		reducers: nil,
	}

	return
}

//Errors
var ()
