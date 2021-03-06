package gomessagestore

import (
	"context"
)

// GetMessages retrieves messages from the message store; Second process in the polling loop
func (sw *subscriptionWorker) GetMessages(ctx context.Context, position int64) ([]Message, error) {
	opts := []GetOption{}
	for _, conv := range sw.config.converters {
		opts = append(opts, Converter(conv))
	}
	if !sw.config.stream { // for stream subscription
		opts = append(opts, SincePosition(position))
		if sw.config.commandCategory != "" { // for commands
			opts = append(opts, CommandCategory(sw.config.commandCategory))
		} else { // for events
			opts = append(opts, Category(sw.config.category))
		}
	} else { // for category subscription
		opts = append(opts, SinceVersion(position))
		if sw.config.commandCategory != "" { // for commands
			opts = append(opts, CommandStream(sw.config.commandCategory))
		} else { // for events
			opts = append(opts, EventStream(sw.config.category, sw.config.entityID))
		}
	}

	return sw.ms.Get(ctx, opts...)
}
