# Go Message Store Connector
eventide interface for go

## Writing to a message store

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
OR
```
// NewMessageStoreFromRepository creates a new MessageStore instance using an injected repository.
func NewMessageStoreFromRepository(injectedRepo repository.Repository) MessageStore {
	msgstr := &msgStore{
		repo: injectedRepo,
	}

	return msgstr
}
```

### Example

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

### Creating a subscriber

### Example

### Tips and tricks

## Projecting from streams

### Creating a projector

### Example

### Tips and tricks
