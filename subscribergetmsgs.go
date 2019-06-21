package gomessagestore

import (
	"context"
)

//GetMessages phase two
func (sw *subscriptionWorker) GetMessages(ctx context.Context, position int64) ([]Message, error) {
	opts := []GetOption{}
	if sw.sub.config.entityID == "" {
		opts = append(opts, SincePosition(position), Category(sw.sub.config.category))
	} else {
		opts = append(opts, SinceVersion(position))
		if sw.sub.config.commandCategory != "" {
			opts = append(opts, CommandStream(sw.sub.config.commandCategory))
		} else {
			opts = append(opts, EventStream(sw.sub.config.category, sw.sub.config.entityID))
		}
	}

	return sw.ms.Get(ctx, opts...)
}
