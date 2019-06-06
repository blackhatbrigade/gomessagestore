package projector

import (

  "golang.org/x/net/context"
  "github.com/blackhatbrigade/gomessagestore/repository"
)

//ReducerOption Variadic parameter support for reducers.
type reducerOption func(proj *projector)

//Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
  RegisterReducer(reducer *MessageReducer, opts ...reducerOption) error
  Run(ctx context.Context) error
}

//projector The base supported projector struct.
type projector struct {
  repo  repository.Repository
}

//CreateProjector Creates a default projector struct that conforms to the interface.
func CreateProjector(repos repository.Repository) (proj Projector) {
  proj = &projector{
    repo : repos,
  }

  return
}

//Errors
var (
)
