# Go Message Store Connector
eventide interface for go

## Writing to a message store

### Writer description

A writer is a function on a message store instance that can write messages as commands or events to the message store database.

### Creating a writer

#### Instantiate a MessageStore instance:
```
// NewMessageStore creates a new MessageStore instance using an injected DB.
func NewMessageStore(injectedDB *sql.DB) MessageStore {
	pgRepo := repository.NewPostgresRepository(injectedDB)

	msgstr := &msgStore{
		repo: pgRepo,
	}

	return msgstr
}
``` 

#### Example

```
import ( 
    gms "github.com/blackhatbrigade/gomessagestore"
    
    "context"
)

func NewEvent() *gms.Event {
    return &gms.Event{}
}

func Process(ctx context.Context, ms gms.MessageStore, msg gms.Message) error {
    data := {}

    packedData, err := gms.Pack(data)

    newEvent := NewEvent()

    err = ms.Write(ctx, newEvent, gms.AtPosition(-1))

    if err != nil {
        return err
    }

    return nil
}
```

### Tips and tricks

## Subscribing to streams and categories

### Subscriber description

A subscriber is used to retrieve new messages from a specified category or stream. It should only subscribe to a single category or stream. If the specified stream/category has new messages that have not yet been sent to the subscriber, they will be sent in the next poll iteration.

### Creating a subscriber

Use the CreateSubscriber() function on a messageStore instance.

Some things to note:

subscriberOptions are set by injecting any of the following functions into the params of the CreateSubscriber function:
    SubscribeToEntityStream
    SubscribeToCommandStream
    SubscribeToCategory
    PollTime
    PollErrorDelay
    UpdatePositionEvery
    SubscribeBatchSize

See subscriber_options.go for more details on these functions.

In the example below, we set the category being subscribed to, as well as our batch size using the subscriber options functions.

### Example

```
import (
    gms "github.com/blackhatbrigade/gomessagestore"
    "github.com/blackhatbrigade/gomessagestore/uuid"

    "context"
)

// Create a new messageStore instnace
messageStore := gms.NewMessageStore(postgresDB)

// Set up context for handling our routines
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Create our subscriber
subscriber, error := messageStore.CreateSubscriber(
    "subscriberID", 
    []gms.MessageHandler{},
    gms.SubscribeToCategory("categoryID"),
    gms.SubscribeBatchSize(500),
)

if error != nil {
    return error
}

// Run our subscriber; our handlers will take care of processing the incoming messages.
go subscriber.Start(ctx)
```

### Tips and tricks

## Projecting from streams

### Projector description

A projector allows you to take an initial state and update it by processing all of the messages in a specific stream in order to derive the current state of the stream.

### Creating a projector

Use the CreateProjector() function on a messageStore instance.

Some things to note:

ProjectorOptions are set by injecting the following functions into the params of the CreateProjector function:
    WithReducer
    DefaultState

See projector.go for more details on these functions.

### Example

```
import (
    gms "github.com/blackhatbrigade/gomessagestore"
    "github.com/blackhatbrigade/gomessagestore/uuid"

    "context"
)

// Create a new messageStore instnace
messageStore := gms.NewMessageStore(postgresDB)

projector, err := messageStore.CreateProjector(
    gms.DefaultState(someStruct{}),
    gms.WithReducer(reducer1),
    gms.WithReducer(reducer2),
)
```

### Tips and tricks

projectors are typically passed into handlers. Here is an example of a generic handler function that ingests a projector as one of its parameters:

```
func genericHandler(ctx context.Context, repo Repository, projector gms.Projector, msg gms.Message, expectedType string) error {
	event, ok := msg.(*gms.Event)
	if !ok || event.Type() != expectedType {
		return ErrInvalidEventTypeInHandler
	}

	projection, err := projector.Run(ctx, event.StreamCategory, event.EntityID)
	if err != nil {
		return err
	}

	property, ok := projection.(PropertyDetail)
	if !ok {
		return ErrInvalidTypeFromProjection
	}

	// the projector already set everything up for us, so just store it
	if err := repo.Store(ctx, &property); err != nil {
		var eventMetadata messageMetadata
		err := gms.Unpack(event.Metadata, &eventMetadata)
		if err != nil {
			Log.WithError(err).Error("While unpacking event metadata an error occurred")
		}

		Log.WithMeta(eventMetadata).WithError(err).Errorf("Couldn't store projection for %s", expectedType)

		return err
	}

	return nil
}
```