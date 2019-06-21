package gomessagestore

import (
	"context"
)

//Start Handles polling at specified intervals
func (sub *subscriber) Start(ctx context.Context) error {
	return sub.pol.Poll(ctx)
}
