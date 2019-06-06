package projector

import (
  "golang.org/x/net/context"
  "github.com/blackhatbrigade/gomessagestore/repository"
)

//Projector A base level interface that defines the projection functionality of gomessagestore.
type Projector interface {
  RegisterReducer()
  Run(ctx Context)
}

//projector The base supported projector struct.
type projector struct {
  repo  Repository
}

//Errors
var (
)
