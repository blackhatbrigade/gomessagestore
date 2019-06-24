package gomessagestore

import (
	"context"
)

//Start Handles polling at specified intervals
func (sub *subscriber) Start(ctx context.Context, out chan<- Value) error {
	polling, err := sub.poller.Poll(ctx)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- polling:
	}
}
