package gomessagestore

import (
	"context"
)

//GetMessages phase two
func (sw *subscriptionWorker) GetMessages(ctx context.Context, position int64) ([]Message, error) {
	opts := []GetOption{}
	if !sw.config.stream {
		opts = append(opts, SincePosition(position), Category(sw.config.category))
	} else {
		opts = append(opts, SinceVersion(position))
		if sw.config.commandCategory != "" {
			opts = append(opts, CommandStream(sw.config.commandCategory))
		} else {
			opts = append(opts, EventStream(sw.config.category, sw.config.entityID))
		}
	}

	return sw.ms.Get(ctx, opts...)
}
