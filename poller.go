package gomessagestore

import (
	"context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore Poller > mocks/poller.go"

type Poller interface {
	Poll(context.Context) error
}

type poller struct {
	opts   *SubscriberConfig
	ms     MessageStore
	worker SubscriptionWorker
}

func CreatePoller(ms MessageStore, worker SubscriptionWorker, opts *SubscriberConfig) (*poller, error) {
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

	//	numberOfMsgsHandled, posOfLastHandled, err = worker.ProcessMessages(ctx, msgs)

	_, _, err = worker.ProcessMessages(ctx, msgs)
	for _, msg := range msgs {
		err = worker.SetPosition(ctx, msg)
		if err != nil {
			return err
		}
	}

	return err
}
