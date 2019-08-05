# Go Message Store Connector
For GoDoc documentation, click [here](https://godoc.org/github.com/blackhatbrigade/gomessagestore)\n \n
postgres eventide interface for go

## Writing to a message store

### Writer description

A writer is a function on a message store instance that can write messages as commands or events to the message store database.

### Creating a writer

#### Example

```
import ( 
    gms "github.com/blackhatbrigade/gomessagestore"
    
    "context"
)

func Process(ctx context.Context, ms gms.MessageStore, msg gms.Message) error {
    data := {}

    packedData, err := gms.Pack(data)

    newEvent := NewEvent()

    // attempt to write the message to the message store. If an error occurs, return the error.
    err = ms.Write(ctx, newEvent, gms.AtPosition(-1))

    if err != nil {
        return err
    }

    return nil
}

messageStore = gms.NewMessageStore(postgresDBInstance)

ctx, cancel := context.WithCancel(context.Background())

msg = someMessage

err := Process(ctx, messageStore, msg)
```

### Tips and tricks

## Subscribing to streams and categories

### Subscriber description

A subscriber is used to retrieve new messages from a specified category or stream. It should only subscribe to a single category or stream. If the specified stream/category has new messages that have not yet been sent to the subscriber, they will be sent in the next poll iteration.

### Creating a subscriber

Use the CreateSubscriber() function on a messageStore instance.

Some things to note:

[subscriberOptions](https://godoc.org/github.com/blackhatbrigade/gomessagestore#SubscriberOption) are set by injecting any of the following functions into the params of the CreateSubscriber function:
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

[ProjectorOptions](https://godoc.org/github.com/blackhatbrigade/gomessagestore#ProjectorOption) are set by injecting the following functions into the params of the CreateProjector function:
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

projectors are typically passed into handlers. Here is a good example of an aggregator handler that ingests a projector as one of its parameters:

```
func genericHandler(ctx context.Context, repo ReadModelDatabase, projector gms.Projector, msg gms.Message, expectedType string) error {
	event, ok := msg.(*gms.Event)
	if !ok || event.Type() != expectedType {
		return ErrInvalidEventTypeInHandler
	}

	projection, err := projector.Run(ctx, event.StreamCategory, event.EntityID)
	if err != nil {
		return err
	}

	entity, ok := projection.(EntityDetail)
	if !ok {
		return ErrInvalidTypeFromProjection
	}

	// the projector already set everything up for us, so just store it
	error := ReadModelDatabase.store(entity)
    if error != nil {
        return databaseWriteError
    }

	return nil
}
```

## UUID package

GO MESSAGE STORE includes a built in package for generating UUID's that you can use for message IDs.

### Example
```
import ( uuid "github.com/blackhatbrigade/gomessagestore/uuid" )

// returns a random V4 UUID
uuid := uuid.NewRandom()
```