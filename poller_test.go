package gomessagestore_test

import (
	"context"
	"testing"

	. "github.com/blackhatbrigade/gomessagestore"
	mock_gomessagestore "github.com/blackhatbrigade/gomessagestore/mocks"
	mock_repository "github.com/blackhatbrigade/gomessagestore/repository/mocks"
	"github.com/golang/mock/gomock"
)

type getMessagesParams struct {
	position int64
}
type getMessagesReturns struct {
	messages []Message
	err      error
}

type processMessagesParams struct {
	messages []Message
}
type processMessagesReturns struct {
	msgsHandled int
	lastPos     int64
	err         error
}

type setPositionParams struct {
	position int64
}
type setPositionReturns struct {
	err error
}

func TestPoller(t *testing.T) {

	tests := []struct {
		name               string
		expectedError      error
		subOpts            []SubscriberOption
		handlers           []MessageHandler
		foundPosition      int64
		getMsgsParams      []getMessagesParams
		getMsgsReturns     []getMessagesReturns
		processMsgsParams  []processMessagesParams
		processMsgsReturns []processMessagesReturns
		setPosParams       []setPositionParams
		setPosReturns      []setPositionReturns
		foundPositionError error
		callPollNumTimes   int
	}{{
		name: "It ran",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		handlers:           []MessageHandler{},
		callPollNumTimes:   1,
		getMsgsParams:      []getMessagesParams{{0}},
		getMsgsReturns:     []getMessagesReturns{{commandsToMessageSlice(getSampleCommands()), nil}},
		processMsgsParams:  []processMessagesParams{{commandsToMessageSlice(getSampleCommands())}},
		processMsgsReturns: []processMessagesReturns{{2, 1012, nil}},
	}, {
		name: "GetPosition Errors are returned",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		handlers:           []MessageHandler{},
		foundPositionError: potato,
		expectedError:      potato,
		callPollNumTimes:   1,
	}, {
		name: "GetMessages Errors are returned",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		handlers:         []MessageHandler{},
		expectedError:    potato,
		callPollNumTimes: 1,
		getMsgsParams:    []getMessagesParams{{0}},
		getMsgsReturns:   []getMessagesReturns{{commandsToMessageSlice(getSampleCommands()), potato}},
	}, {
		name: "ProcessMessages Errors are returned",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		handlers:           []MessageHandler{},
		expectedError:      potato,
		callPollNumTimes:   1,
		getMsgsParams:      []getMessagesParams{{0}},
		getMsgsReturns:     []getMessagesReturns{{commandsToMessageSlice(getSampleCommands()), nil}},
		processMsgsParams:  []processMessagesParams{{commandsToMessageSlice(getSampleCommands())}},
		processMsgsReturns: []processMessagesReturns{{2, 1012, potato}},
	}, {
		name: "SetPosition Errors are returned",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
			UpdatePositionEvery(7),
		},
		handlers:           []MessageHandler{},
		expectedError:      potato,
		callPollNumTimes:   1,
		getMsgsParams:      []getMessagesParams{{0}},
		getMsgsReturns:     []getMessagesReturns{{commandsToMessageSlice(getSampleCommands()), nil}},
		processMsgsParams:  []processMessagesParams{{commandsToMessageSlice(getSampleCommands())}},
		processMsgsReturns: []processMessagesReturns{{10, 1012, nil}},
		setPosParams:       []setPositionParams{{1012}},
		setPosReturns:      []setPositionReturns{{potato}},
	}, {
		name: "When called twice, Poll uses a changed value for starting position",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
		},
		handlers:         []MessageHandler{},
		callPollNumTimes: 2,
		getMsgsParams: []getMessagesParams{
			{0},
			{1012},
		},
		getMsgsReturns: []getMessagesReturns{
			{commandsToMessageSlice(getSampleCommands()), nil},
			{commandsToMessageSlice(getSampleCommands()), nil},
		},
		processMsgsParams: []processMessagesParams{
			{commandsToMessageSlice(getSampleCommands())},
			{commandsToMessageSlice(getSampleCommands())},
		},
		processMsgsReturns: []processMessagesReturns{
			{5, 1012, nil},
			{5, 9000, nil},
		},
	}, {
		name: "SetPosition is called when the correct amount of messages are processed",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
			UpdatePositionEvery(7),
		},
		handlers:         []MessageHandler{},
		callPollNumTimes: 2,
		getMsgsParams: []getMessagesParams{
			{0},
			{1012},
		},
		getMsgsReturns: []getMessagesReturns{
			{commandsToMessageSlice(getSampleCommands()), nil},
			{commandsToMessageSlice(getSampleCommands()), nil},
		},
		processMsgsParams: []processMessagesParams{
			{commandsToMessageSlice(getSampleCommands())},
			{commandsToMessageSlice(getSampleCommands())},
		},
		processMsgsReturns: []processMessagesReturns{
			{5, 1012, nil},
			{5, 9000, nil},
		},
		setPosParams:  []setPositionParams{{9000}},
		setPosReturns: []setPositionReturns{{nil}},
	}, {
		name: "SetPosition is called (multiple times) when the correct amount of messages are processed",
		subOpts: []SubscriberOption{
			SubscribeToCommandStream("some cat"),
			UpdatePositionEvery(5),
		},
		handlers:         []MessageHandler{},
		callPollNumTimes: 3,
		getMsgsParams: []getMessagesParams{
			{0},
			{1012},
			{9000},
		},
		getMsgsReturns: []getMessagesReturns{
			{commandsToMessageSlice(getSampleCommands()), nil},
			{commandsToMessageSlice(getSampleCommands()), nil},
			{commandsToMessageSlice(getSampleCommands()), nil},
		},
		processMsgsParams: []processMessagesParams{
			{commandsToMessageSlice(getSampleCommands())},
			{commandsToMessageSlice(getSampleCommands())},
			{commandsToMessageSlice(getSampleCommands())},
		},
		processMsgsReturns: []processMessagesReturns{
			{5, 1012, nil},
			{3, 9000, nil},
			{2, 1000000, nil},
		},
		setPosParams: []setPositionParams{
			{1012},
			{1000000},
		},
		setPosReturns: []setPositionReturns{
			{nil},
			{nil},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()

			// mocks and expectations
			mockRepo := mock_repository.NewMockRepository(ctrl)

			myWorker := mock_gomessagestore.NewMockSubscriptionWorker(ctrl)

			myWorker.
				EXPECT().
				GetPosition(ctx).
				Return(test.foundPosition, test.foundPositionError)

			var lastCall *gomock.Call
			for index, _ := range test.getMsgsParams {
				thisCall := myWorker.
					EXPECT().
					GetMessages(ctx, test.getMsgsParams[index].position).
					Return(test.getMsgsReturns[index].messages, test.getMsgsReturns[index].err)
				if lastCall != nil {
					thisCall.After(lastCall)
				}
				lastCall = thisCall
			}
			lastCall = nil

			for index, _ := range test.processMsgsParams {
				thisCall := myWorker.
					EXPECT().
					ProcessMessages(ctx, test.processMsgsParams[index].messages).
					Return(test.processMsgsReturns[index].msgsHandled, test.processMsgsReturns[index].lastPos, test.processMsgsReturns[index].err)
				if lastCall != nil {
					thisCall.After(lastCall)
				}
				lastCall = thisCall
			}
			lastCall = nil

			for index, _ := range test.setPosParams {
				thisCall := myWorker.
					EXPECT().
					SetPosition(ctx, test.setPosParams[index].position).
					Return(test.setPosReturns[index].err)
				if lastCall != nil {
					thisCall.After(lastCall)
				}
				lastCall = thisCall
			}
			lastCall = nil

			// setup
			myMessageStore := NewMessageStoreFromRepository(mockRepo)
			opts, err := GetSubscriberConfig(test.subOpts...)
			myPoller, err := CreatePoller(
				myMessageStore,
				myWorker,
				opts,
			)
			if err != nil {
				t.Errorf("Failed on CreatePoller() Got: %s\n", err)
				return
			}

			// call
			for c := 0; c < test.callPollNumTimes; c++ {
				err = myPoller.Poll(ctx)

				// assertions
				if err != test.expectedError {
					t.Errorf("Failed on Poll()\nWant: %s\nHave: %s\n", test.expectedError, err)
					return
				}
			}
		})
	}
}
