package metrics

import (
	"context"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

// InstrumentListenerClient wraps a listener.Client so that every message poll
// (GetMessage call) is recorded on the recorder: the poll result (message,
// empty or error), the time of the poll and its duration.
//
// All other calls are passed through to the wrapped client unchanged.
func InstrumentListenerClient(client listener.Client, recorder Recorder) listener.Client {
	if recorder == nil {
		recorder = Discard
	}
	return &instrumentedListenerClient{
		Client:   client,
		recorder: recorder,
		now:      time.Now,
	}
}

type instrumentedListenerClient struct {
	listener.Client
	recorder Recorder
	now      func() time.Time
}

var _ listener.Client = &instrumentedListenerClient{}

func (c *instrumentedListenerClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*scaleset.RunnerScaleSetMessage, error) {
	start := c.now()
	msg, err := c.Client.GetMessage(ctx, lastMessageID, maxCapacity)
	duration := c.now().Sub(start)

	// Do not count polls interrupted by the listener shutting down.
	if err != nil && ctx.Err() != nil {
		return msg, err
	}

	result := PollResultMessage
	switch {
	case err != nil:
		result = PollResultError
	case msg == nil:
		result = PollResultEmpty
	}
	c.recorder.RecordPoll(result, duration)

	return msg, err
}
