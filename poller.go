package gomessagestore

import (
	"context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Poller > mocks/poller.go"

// Poller interface requires a Poll function
type Poller interface {
	Poll(context.Context) error // should handle a cycle of polling the message store
}

type poller struct {
	config              *SubscriberConfig
	ms                  MessageStore
	worker              SubscriptionWorker
	position            int64
	numberOfMsgsHandled int
}

// CreatePoller returns a new instance of a Poller
func CreatePoller(ms MessageStore, worker SubscriptionWorker, config *SubscriberConfig) (*poller, error) {
	return &poller{
		config:   config,
		worker:   worker,
		position: -1,
	}, nil
}

//Poll Handles a single tick of the handlers firing
func (pol *poller) Poll(ctx context.Context) error {
	worker := pol.worker
	// use the position of the worker if the poller position is still its default value or an invalid <0 value
	if pol.position < 0 {
		pos, err := worker.GetPosition(ctx)
		if err != nil {
			return err
		}
		pol.position = pos
	}

	msgs, err := worker.GetMessages(ctx, pol.position)
	if err != nil {
		return err
	}

	numberOfMsgsHandled, posOfLastHandled, _ := worker.ProcessMessages(ctx, msgs) // ProcessMessages logs errors but does not return them as the process should continue despite an error occuring
	if numberOfMsgsHandled > 0 {
		pol.position = posOfLastHandled + 1 // update poller with the new position
	}
	pol.numberOfMsgsHandled += numberOfMsgsHandled

	if pol.numberOfMsgsHandled >= pol.config.updateInterval {
		if err = worker.SetPosition(ctx, pol.position); err != nil {
			return err
		}
		pol.numberOfMsgsHandled = 0
	}

	return nil
}
