package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/actions/scaleset"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var scaleSetGaugeLabels = []string{
	labelKeyEnterprise,
	labelKeyOrganization,
	labelKeyRepository,
	labelKeyRunnerScaleSetName,
	labelKeyRunnerScaleSetNamespace,
}

var pollMetricsConfig = v1alpha1.MetricsConfig{
	Counters: map[string]*v1alpha1.CounterMetric{
		MetricListenerPollsTotal: {
			Labels: append(append([]string{}, scaleSetGaugeLabels...), labelKeyPollResult),
		},
	},
	Gauges: map[string]*v1alpha1.GaugeMetric{
		MetricListenerLastPollTimestampSeconds:      {Labels: scaleSetGaugeLabels},
		MetricListenerLastPollDurationSeconds:       {Labels: scaleSetGaugeLabels},
		MetricStatisticsLastUpdatedTimestampSeconds: {Labels: scaleSetGaugeLabels},
		MetricRegisteredRunners:                     {Labels: scaleSetGaugeLabels},
	},
}

func gaugeValue(t *testing.T, metrics map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	mf, ok := metrics[name]
	require.True(t, ok, "metric %q not found", name)
	require.Len(t, mf.GetMetric(), 1, "metric %q", name)
	return mf.GetMetric()[0].GetGauge().GetValue()
}

// pollCounts returns the polls counter value keyed by the poll_result label.
func pollCounts(t *testing.T, metrics map[string]*dto.MetricFamily) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	mf, ok := metrics[MetricListenerPollsTotal]
	if !ok {
		return out
	}
	for _, m := range mf.GetMetric() {
		var result string
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelKeyPollResult {
				result = lp.GetValue()
			}
		}
		out[result] = m.GetCounter().GetValue()
	}
	return out
}

func TestRecordPoll(t *testing.T) {
	e, reg := newTestExporter(t, pollMetricsConfig)
	now := time.Unix(1790700000, 500_000_000)
	e.now = func() time.Time { return now }

	e.RecordPoll(PollResultEmpty, 50*time.Second)
	e.RecordPoll(PollResultEmpty, 50*time.Second)
	e.RecordPoll(PollResultMessage, 2*time.Second)
	e.RecordPoll(PollResultError, 1500*time.Millisecond)

	metrics := gatherMetrics(t, reg)
	assert.Equal(t, map[string]float64{
		string(PollResultEmpty):   2,
		string(PollResultMessage): 1,
		string(PollResultError):   1,
	}, pollCounts(t, metrics))
	assert.Equal(t, 1790700000.5, gaugeValue(t, metrics, MetricListenerLastPollTimestampSeconds))
	assert.Equal(t, 1.5, gaugeValue(t, metrics, MetricListenerLastPollDurationSeconds), "duration of the most recent poll")

	// Statistics are not touched by a poll.
	_, ok := metrics[MetricStatisticsLastUpdatedTimestampSeconds]
	assert.False(t, ok, "polling must not update the statistics timestamp")
}

func TestRecordStatisticsSetsLastUpdatedTimestamp(t *testing.T) {
	e, reg := newTestExporter(t, pollMetricsConfig)
	now := time.Unix(1790700000, 0)
	e.now = func() time.Time { return now }

	e.RecordStatistics(&scaleset.RunnerScaleSetStatistic{TotalRegisteredRunners: 1})

	metrics := gatherMetrics(t, reg)
	assert.Equal(t, 1.0, gaugeValue(t, metrics, MetricRegisteredRunners))
	assert.Equal(t, 1790700000.0, gaugeValue(t, metrics, MetricStatisticsLastUpdatedTimestampSeconds))

	now = now.Add(time.Minute)
	e.RecordStatistics(&scaleset.RunnerScaleSetStatistic{TotalRegisteredRunners: 2})

	metrics = gatherMetrics(t, reg)
	assert.Equal(t, 2.0, gaugeValue(t, metrics, MetricRegisteredRunners))
	assert.Equal(t, 1790700060.0, gaugeValue(t, metrics, MetricStatisticsLastUpdatedTimestampSeconds))
}

func TestRecordPollWithMetricsDisabled(t *testing.T) {
	// Metrics that are not configured must be silently skipped.
	e, reg := newTestExporter(t, v1alpha1.MetricsConfig{})
	e.RecordPoll(PollResultEmpty, time.Second)
	e.RecordStatistics(&scaleset.RunnerScaleSetStatistic{})
	assert.Empty(t, gatherMetrics(t, reg))
}

// fakeListenerClient is a minimal listener.Client used to drive the
// instrumented client.
type fakeListenerClient struct {
	msg      *scaleset.RunnerScaleSetMessage
	err      error
	polls    int
	deletes  []int
	acquired []int64
	session  scaleset.RunnerScaleSetSession
}

func (f *fakeListenerClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*scaleset.RunnerScaleSetMessage, error) {
	f.polls++
	return f.msg, f.err
}

func (f *fakeListenerClient) DeleteMessage(ctx context.Context, messageID int) error {
	f.deletes = append(f.deletes, messageID)
	return nil
}

func (f *fakeListenerClient) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	f.acquired = append(f.acquired, requestIDs...)
	return requestIDs, nil
}

func (f *fakeListenerClient) Session() scaleset.RunnerScaleSetSession {
	return f.session
}

// steppingClock returns start, then start+step, start+2*step, ...
func steppingClock(start time.Time, step time.Duration) func() time.Time {
	next := start
	return func() time.Time {
		now := next
		next = next.Add(step)
		return now
	}
}

func TestInstrumentListenerClient(t *testing.T) {
	pollErr := errors.New("boom")

	tests := []struct {
		name       string
		msg        *scaleset.RunnerScaleSetMessage
		err        error
		wantResult PollResult
	}{
		{
			name:       "message",
			msg:        &scaleset.RunnerScaleSetMessage{MessageID: 7},
			wantResult: PollResultMessage,
		},
		{
			name:       "empty poll (202)",
			msg:        nil,
			wantResult: PollResultEmpty,
		},
		{
			name:       "error",
			err:        pollErr,
			wantResult: PollResultError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &fakeListenerClient{msg: tt.msg, err: tt.err}
			recorder := NewMockRecorder(t)
			recorder.EXPECT().RecordPoll(tt.wantResult, 50*time.Second).Once()

			client := InstrumentListenerClient(inner, recorder).(*instrumentedListenerClient)
			client.now = steppingClock(time.Unix(1790700000, 0), 50*time.Second)

			msg, err := client.GetMessage(t.Context(), 1, 10)
			assert.Same(t, tt.msg, msg)
			assert.Equal(t, tt.err, err)
			assert.Equal(t, 1, inner.polls)
		})
	}
}

func TestInstrumentListenerClientSkipsShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	inner := &fakeListenerClient{err: context.Canceled}
	recorder := NewMockRecorder(t) // no expectations: RecordPoll must not be called

	msg, err := InstrumentListenerClient(inner, recorder).GetMessage(ctx, 0, 10)
	assert.Nil(t, msg)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestInstrumentListenerClientPassesThrough(t *testing.T) {
	session := scaleset.RunnerScaleSetSession{OwnerName: "owner"}
	inner := &fakeListenerClient{session: session}
	client := InstrumentListenerClient(inner, NewMockRecorder(t))

	require.NoError(t, client.DeleteMessage(t.Context(), 42))
	acquired, err := client.AcquireJobs(t.Context(), []int64{1, 2})
	require.NoError(t, err)

	assert.Equal(t, []int{42}, inner.deletes)
	assert.Equal(t, []int64{1, 2}, acquired)
	assert.Equal(t, session, client.Session())
}

func TestInstrumentListenerClientNilRecorder(t *testing.T) {
	inner := &fakeListenerClient{}
	client := InstrumentListenerClient(inner, nil)

	msg, err := client.GetMessage(t.Context(), 0, 10)
	assert.Nil(t, msg)
	assert.NoError(t, err)
}
