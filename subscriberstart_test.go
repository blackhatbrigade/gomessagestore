package gomessagestore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_gomessagestore "github.com/blackhatbrigade/gomessagestore/mocks"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

func TestSubscriberStartWithContext(t *testing.T) {
	tests := []struct {
		name                string
		handlers            []MessageHandler
		expectedError       error
		pollError           error
		expectedTimesPolled int
		sleepyTime          time.Duration
		messages            []Message
		opts                []SubscriberOption
	}{{
		name:                "Should cancel when asked, nicely",
		handlers:            []MessageHandler{&msgHandler{}},
		sleepyTime:          200 * time.Millisecond,
		expectedTimesPolled: 1,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
	}, {
		name:                "When Poll() returns an error, start continues to call the Poll() function",
		handlers:            []MessageHandler{&msgHandler{}},
		pollError:           errors.New("I'm an erorr"),
		sleepyTime:          20 * time.Millisecond, // will take 40 ms to run twice, so cancel will happen during the second run
		expectedTimesPolled: 2,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
			PollTime(1),
		},
	}, {
		name:                "Waits between Poll() calls",
		handlers:            []MessageHandler{&msgHandler{}},
		pollError:           errors.New("I'm an erorr"),
		sleepyTime:          20 * time.Millisecond, // will take 40 ms to run twice, so cancel will happen during the second run, but our default delay add enough wait for it to only be called once
		expectedTimesPolled: 1,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			mockRepo := mock_repository.NewMockRepository(ctrl)
			mockPoller := mock_gomessagestore.NewMockPoller(ctrl)

			var wg sync.WaitGroup

			wg.Add(test.expectedTimesPolled)

			mockPoller.
				EXPECT().
				Poll(ctx).
				Do(func(ctx context.Context) {
					wg.Done()
					time.Sleep(test.sleepyTime)
				}).
				Return(test.pollError).
				AnyTimes()

			myMessageStore := NewMessageStoreFromRepository(mockRepo)

			mySubscriber, err := CreateSubscriberWithPoller(
				myMessageStore,
				"someid",
				test.handlers,
				mockPoller,
				test.opts...,
			)
			if err != nil {
				t.Errorf("Failed on CreateSubscriber() Got: %s\n", err)
			}

			finished := make(chan error, 1)
			go func() {
				err = mySubscriber.Start(ctx)
				finished <- err
			}()

			time.Sleep(30 * time.Millisecond)
			cancel()

			test.expectedError = ctx.Err()
			select {
			case err := <-finished:
				if err != test.expectedError {
					t.Errorf("Failed to get expected error from ProcessMessages()\nExpected: %s\n and got: %s\n", test.expectedError, err)
				}
			case <-time.After(60 * time.Millisecond):
				t.Error("Timed out")
			}
			if waitTimeout(&wg, 500*time.Millisecond) {
				t.Errorf("Failed to meet expected number of calls to Poll()\nExpected: %d\n", test.expectedTimesPolled)
			}
		})
	}
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
