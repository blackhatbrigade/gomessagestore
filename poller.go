package gomessagestore

import (
	"context"
)

type Poller interface {
	Poll(context.Context) error
}

type poller struct {
	opts   []SubscriberOption
	ms     MessageStore
	worker *subscriptionWorker
}

func CreatePoller(ms MessageStore, worker *subscriptionWorker, opts ...SubscriberOption) (*poller, error) {
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

	//numberOfMsgsHandled, posOfLastHandled, err = worker.ProcessMessages(ctx, msgs)

	_, _, err = worker.ProcessMessages(ctx, msgs)
	//err = worker.SetPosition(ctx, msgs)

	return err
}
