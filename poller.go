package gomessagestore

import (
	"context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Poller > mocks/poller.go"

type Poller interface {
	Poll(context.Context) error
}

type poller struct {
	config              *SubscriberConfig
	ms                  MessageStore
	worker              SubscriptionWorker
	position            int64
	numberOfMsgsHandled int
}

func CreatePoller(ms MessageStore, worker SubscriptionWorker, config *SubscriberConfig) (*poller, error) {
	return &poller{
		ms:       ms,
		config:   config,
		worker:   worker,
		position: -1,
	}, nil
}

//Poll Handles a single tick of the handlers firing
func (pol *poller) Poll(ctx context.Context) error {
	worker := pol.worker
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

	numberOfMsgsHandled, posOfLastHandled, _ := worker.ProcessMessages(ctx, msgs) // ProcessMessages logs errors as it goes, so I'm not looking at the error (nor does ProcessMessage ever return any error)
	if numberOfMsgsHandled > 0 {
		pol.position = posOfLastHandled + 1
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
