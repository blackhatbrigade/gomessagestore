package gomessagestore

import (
	"context"
	"fmt"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Poller > mocks/poller.go"

type Poller interface {
	Poll(context.Context) error
}

type poller struct {
	opts   *SubscriberConfig
	ms     MessageStore
	worker *subscriptionWorker
}

func CreatePoller(ms MessageStore, worker *subscriptionWorker, opts *SubscriberConfig) (*poller, error) {
	return &poller{
		ms:     ms,
		opts:   opts,
		worker: worker,
	}, nil
}

//Poll Handles a single tick of the handlers firing
func (pol poller) Poll(ctx context.Context) error {
	worker := pol.worker

	pos, err := worker.GetPosition(ctx)
	if err != nil {
		return err
	}

	msgs, err := worker.GetMessages(ctx, pos)
	if err != nil {
		return err
	}
	var numberOfMsgsHandled int
	var posOfLastHandled int64

	numberOfMsgsHandled, posOfLastHandled, err = worker.ProcessMessages(ctx, msgs)
	if err != nil {
		return err
	}

	fmt.Println(numberOfMsgsHandled, posOfLastHandled, err)
	err = worker.SetPosition(ctx, msgs)
	if err != nil {
		return err
	}
	return nil
}