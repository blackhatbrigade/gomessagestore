package gomessagestore_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_gomessagestore "github.com/blackhatbrigade/gomessagestore/mocks"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
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
		cancelDelay         time.Duration
	}{{
		name:                "Should cancel when asked, nicely",
		handlers:            []MessageHandler{&msgHandler{}},
		sleepyTime:          200 * time.Millisecond,
		expectedTimesPolled: 1,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
		cancelDelay: 35 * time.Millisecond,
	}, {
		name:                "When there is no error, start continues to call the Poll() function",
		handlers:            []MessageHandler{&msgHandler{}},
		sleepyTime:          20 * time.Millisecond, // will take 40 ms to run twice, so cancel will happen during the second run
		expectedTimesPolled: 2,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
			PollTime(1),
		},
		cancelDelay: 35 * time.Millisecond,
	}, {
		name:                "Waits between Poll() calls",
		handlers:            []MessageHandler{&msgHandler{}},
		pollError:           errors.New("I'm an erorr"),
		sleepyTime:          20 * time.Millisecond, // will take 40 ms to run twice, so cancel will happen during the second run, but our default delay add enough wait for it to only be called once
		expectedTimesPolled: 1,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
		},
		cancelDelay: 35 * time.Millisecond,
	}, {
		name:                "When Poll() returns an error, start continues to call the Poll() function, after a long delay",
		handlers:            []MessageHandler{&msgHandler{}},
		pollError:           errors.New("I'm an erorr"),
		sleepyTime:          20 * time.Millisecond, // will take 40 ms to run twice, so cancel will happen during the second run
		expectedTimesPolled: 2,
		opts: []SubscriberOption{
			SubscribeToCategory("category"),
			PollTime(1),
			PollErrorDelay(100 * time.Millisecond),
		},
		cancelDelay: 35*time.Millisecond + 100*time.Millisecond,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			mockRepo := mock_repository.NewMockRepository(ctrl)
			mockPoller := mock_gomessagestore.NewMockPoller(ctrl)

			ranTimes := 0
			count := make(chan int, 1)

			mockPoller.
				EXPECT().
				Poll(ctx).
				Do(func(ctx context.Context) {
					count <- 1
					time.Sleep(test.sleepyTime)
				}).
				Return(test.pollError).
				AnyTimes()

			var logrusLogger = &logrus.Logger{
				Out:       os.Stderr,
				Formatter: new(logrus.JSONFormatter),
				Hooks:     make(logrus.LevelHooks),
				Level:     logrus.DebugLevel,
			}

			myMessageStore := NewMessageStoreFromRepository(mockRepo, logrusLogger)

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

			time.AfterFunc(test.cancelDelay, func() {
				cancel()
			})

			done := false
			for !done {
				select {
				case err := <-finished:
					test.expectedError = ctx.Err()
					if err != test.expectedError {
						t.Errorf("Failed to get expected error from ProcessMessages()\nExpected: %s\n and got: %s\n", test.expectedError, err)
					}
					done = true
				case <-time.After(1 * time.Second):
					t.Error("Timed out")
					done = true
				case c := <-count:
					ranTimes += c
				}
			}
			if ranTimes != test.expectedTimesPolled {
				t.Errorf("Failed to meet expected number of calls to Poll()\nHave: %d\nWant: %d\n", ranTimes, test.expectedTimesPolled)
			}
		})
	}
}
