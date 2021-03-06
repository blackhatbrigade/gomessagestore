package gomessagestore

import (
	"context"
	"time"
)

//Start Handles polling at specified intervals
func (sub *subscriber) Start(ctx context.Context) error {

	// make a channel to handle cancel signal from context in order to stop the infinite loop
	cancelled := make(chan error, 1)
	go func() {
		for {
			err := sub.poller.Poll(ctx)
			if err != nil {
				sub.config.log.WithError(err).Error("There is an error with Poller in Start")
				time.Sleep(sub.config.pollErrorDelay)
			}
			select {
			case <-cancelled:
				return
			case <-time.After(sub.config.pollTime):
				// wait between poll
			}
		}
	}()
	select {
	case <-ctx.Done():
		cancelled <- ctx.Err()
		return ctx.Err()
	}
}
