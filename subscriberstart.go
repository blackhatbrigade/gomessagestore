package gomessagestore

import (
	"context"
	"time"
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
			case <-time.After(sub.config.pollTime):
			}
		}
	}()
	select {
	case <-ctx.Done():
		cancelled <- ctx.Err()
		return ctx.Err()
	}
}
