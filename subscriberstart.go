package gomessagestore

import (
	"context"
)

//Start Handles polling at specified intervals
func (sub *subscriber) Start(ctx context.Context) error {
	cancelled := make(chan error, 1)
	go func() {
		for {
			sub.poller.Poll(ctx)
			select {
			case <-cancelled:
				return
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		cancelled <- ctx.Err()
		return ctx.Err()
	}
}
