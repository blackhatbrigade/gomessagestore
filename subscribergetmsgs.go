package gomessagestore

import (
	"context"
)

//GetMessages phase two
func (sub *subscriber) GetMessages(ctx context.Context, position int64) ([]Message, error) {
	opts := []GetOption{}
	if sub.entityID == "" {
		opts = append(opts, SincePosition(position))
		opts = append(opts, Category(sub.category))
	} else {
		opts = append(opts, SinceVersion(position))
		if sub.commandCategory != "" {
			opts = append(opts, CommandStream(sub.commandCategory))
		} else {
			opts = append(opts, EventStream(sub.category, sub.entityID))
		}
	}

	return sub.ms.Get(ctx, opts...)
}
