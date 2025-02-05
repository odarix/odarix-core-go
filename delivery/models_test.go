package delivery_test

//go:generate moq -out models_moq_test.go -pkg delivery_test -rm ../cppbridge Segment

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestPromiseAck(t *testing.T) {
	t.Parallel()

	n := 16
	promise := delivery.NewSendPromise(n)
	for i := 0; i < n; i++ {
		time.AfterFunc(time.Millisecond, promise.Ack)
	}
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.True(t, res)
	assert.NoError(t, err)
}

func TestPromiseRefill(t *testing.T) {
	t.Parallel()

	n := 16
	promise := delivery.NewSendPromise(n)
	for i := 0; i < n; i++ {
		fn := promise.Ack
		if i%2 == 1 {
			fn = promise.Refill
		}
		time.AfterFunc(time.Millisecond, fn)
	}
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.NoError(t, err)
}

func TestPromiseAbort(t *testing.T) {
	t.Parallel()

	n := 16
	promise := delivery.NewSendPromise(n)
	for i := 0; i < n; i++ {
		fn := promise.Ack
		if i%2 == 1 {
			fn = promise.Abort
		}
		time.AfterFunc(time.Millisecond, fn)
	}
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.ErrorIs(t, err, delivery.ErrAborted)
}

func TestPromiseAbort_short(t *testing.T) {
	t.Parallel()

	n := 16
	promise := delivery.NewSendPromise(n)
	time.AfterFunc(time.Millisecond, promise.Abort)
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.ErrorIs(t, err, delivery.ErrAborted)
}

func TestPromiseError(t *testing.T) {
	t.Parallel()

	n := 16
	expectedErr := errors.New("test error")
	promise := delivery.NewSendPromise(n)
	for i := 0; i < n; i++ {
		fn := promise.Ack
		if i%2 == 1 {
			fn = func() {
				promise.Error(expectedErr)
			}
		}
		time.AfterFunc(time.Millisecond, fn)
	}
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.ErrorIs(t, err, expectedErr)
}

func TestPromiseError_short(t *testing.T) {
	t.Parallel()

	n := 16
	expectedErr := errors.New("test error")
	promise := delivery.NewSendPromise(n)
	time.AfterFunc(time.Millisecond, func() {
		promise.Error(expectedErr)
	})
	ctx := context.Background()
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.ErrorIs(t, err, expectedErr)
}

func TestPromiseAwait(t *testing.T) {
	t.Parallel()

	n := 16
	expectedErr := errors.New("test error")
	promise := delivery.NewSendPromise(n)
	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)
	time.AfterFunc(time.Millisecond, func() {
		cancel(expectedErr)
	})
	res, err := promise.Await(ctx)
	assert.False(t, res)
	assert.ErrorIs(t, err, expectedErr)
}

func TestOpenHeadPromise(t *testing.T) {
	suite.Run(t, new(OpenHeadPromiseSuite))
}

type OpenHeadPromiseSuite struct {
	suite.Suite
	clock        *clockwork.FakeClock
	encodersLock *sync.Mutex
	finalized    *atomic.Bool
}

func (s *OpenHeadPromiseSuite) SetupTest() {
	s.clock = clockwork.NewFakeClock()
	s.encodersLock = new(sync.Mutex)
	s.finalized = new(atomic.Bool)
}

func (s *OpenHeadPromiseSuite) TestProlongation() {
	segments := s.getSegmentStats(1e3)
	promise := s.getPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    5 * time.Second,
		MaxSamples:     40e3,
		LastAddTimeout: 200 * time.Millisecond,
	})
	for i := 0; i < 10; i++ {
		s.encodersLock.Lock()
		s.False(promise.Add(segments))
		s.encodersLock.Unlock()
		s.clock.Advance(50 * time.Millisecond)
		time.Sleep(time.Microsecond)
	}
	s.clock.Advance(200 * time.Millisecond)
	select {
	case <-time.After(time.Millisecond):
		s.Fail("promise should finalize")
	case <-promise.Finalized():
	}
	s.True(s.finalized.Load())
}

func (s *OpenHeadPromiseSuite) TestSamplesLimit() {
	segments := s.getSegmentStats(11e3)
	promise := s.getPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    5 * time.Second,
		MaxSamples:     40e3,
		LastAddTimeout: 200 * time.Millisecond,
	})
	for i := 0; i < 3; i++ {
		s.encodersLock.Lock()
		limitsReached, _ := promise.Add(segments)
		s.False(limitsReached)
		s.encodersLock.Unlock()
		s.clock.Advance(50 * time.Millisecond)
		time.Sleep(time.Microsecond)
	}
	s.encodersLock.Lock()
	limitsReached, afterFinish := promise.Add(segments)
	s.True(limitsReached)
	s.finalized.Store(true)
	afterFinish()
	s.encodersLock.Unlock()
	select {
	case <-time.After(time.Millisecond):
		s.Fail("promise should finalize")
	case <-promise.Finalized():
	}
	s.True(s.finalized.Load())
}

func (s *OpenHeadPromiseSuite) TestDurationLimit() {
	segments := s.getSegmentStats(1e3)
	promise := s.getPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    5 * time.Second,
		MaxSamples:     40e3,
		LastAddTimeout: 200 * time.Millisecond,
	})
	for i := 0; i < 30; i++ {
		s.encodersLock.Lock()
		finalized, _ := promise.Add(segments)
		s.False(finalized)
		s.encodersLock.Unlock()
		s.clock.Advance(150 * time.Millisecond)
		time.Sleep(time.Microsecond)
	}
	s.clock.Advance(50 * time.Millisecond)
	select {
	case <-time.After(time.Millisecond):
		s.Fail("promise should finalize")
	case <-promise.Finalized():
	}
	s.True(s.finalized.Load())
}

func (s *OpenHeadPromiseSuite) TestConcurrentClose() {
	segments := s.getSegmentStats(10e3)
	promise := s.getPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    5 * time.Second,
		MaxSamples:     19e3,
		LastAddTimeout: 200 * time.Millisecond,
	})
	s.encodersLock.Lock()
	s.clock.Advance(6 * time.Second)
	time.Sleep(time.Microsecond)
	finalized, afterFinish := promise.Add(segments)
	s.True(finalized)
	afterFinish()
	s.encodersLock.Unlock()
	s.Eventually(s.finalized.Load, time.Second, time.Millisecond)
}

func (s *OpenHeadPromiseSuite) TestPanicOnAddToFinalized() {
	segments := s.getSegmentStats(10e3)
	promise := s.getPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    5 * time.Second,
		MaxSamples:     19e3,
		LastAddTimeout: 200 * time.Millisecond,
	})
	s.clock.Advance(6 * time.Second)
	time.Sleep(time.Microsecond)
	<-promise.Finalized()
	s.Panics(func() {
		promise.Add(segments)
	})
}

func (s *OpenHeadPromiseSuite) TestPanicOnAddToFinalized_Long() {
	if testing.Short() {
		s.T().SkipNow()
	}
	segments := s.getSegmentStats(10e3)
	promise := delivery.NewOpenHeadPromise(len(segments), delivery.OpenHeadLimits{
		MaxDuration:    100 * time.Millisecond,
		MaxSamples:     40e3,
		LastAddTimeout: 200 * time.Millisecond,
	}, clockwork.NewRealClock(), func() {
		s.encodersLock.Lock()
		s.encodersLock.Unlock()
		s.finalized.Store(true)
	}, promauto.With(nil).NewCounter(prometheus.CounterOpts{}))
	s.encodersLock.Lock()
	srnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	delay := time.Duration(90+srnd.Intn(20)) * time.Millisecond
	time.Sleep(delay)
	_, afterFinish := promise.Add(segments)
	afterFinish()
	s.encodersLock.Unlock()
	<-promise.Finalized()
	time.Sleep(10 * time.Millisecond)
}

func (*OpenHeadPromiseSuite) getSegmentStats(samples uint32) []cppbridge.SegmentStats {
	segments := make([]cppbridge.SegmentStats, 4)
	for i := range segments {
		segments[i] = &SegmentMock{
			SamplesFunc: func() uint32 { return samples },
		}
	}
	return segments
}

func (s *OpenHeadPromiseSuite) getPromise(shards int, limits delivery.OpenHeadLimits) *delivery.OpenHeadPromise {
	promise := delivery.NewOpenHeadPromise(shards, limits, s.clock, func() {
		s.encodersLock.Lock()
		defer s.encodersLock.Unlock()

		s.finalized.Store(true)
	}, promauto.With(nil).NewCounter(prometheus.CounterOpts{}))
	return promise
}

type OpenHeadLimitsSuite struct {
	suite.Suite
}

func TestOpenHeadLimits(t *testing.T) {
	suite.Run(t, new(OpenHeadLimitsSuite))
}

func (s *OpenHeadLimitsSuite) TestMarshalBinaryUnmarshalBinary() {
	ohlm := delivery.DefaultOpenHeadLimits()

	b, err := ohlm.MarshalBinary()
	s.NoError(err)

	ohlu := delivery.OpenHeadLimits{}
	err = ohlu.UnmarshalBinary(b)
	s.NoError(err)

	s.Equal(ohlm, ohlu)
}

func (s *OpenHeadLimitsSuite) TestMarshalBinaryUnmarshalBinary_Quick() {
	f := func(maxDuration, lastAddTimeout time.Duration, maxSamples uint32) bool {
		ohlm := delivery.OpenHeadLimits{
			MaxDuration:    maxDuration,
			LastAddTimeout: lastAddTimeout,
			MaxSamples:     maxSamples,
		}

		b, err := ohlm.MarshalBinary()
		s.NoError(err)

		ohlu := delivery.OpenHeadLimits{}
		err = ohlu.UnmarshalBinary(b)
		s.NoError(err)

		return s.Equal(ohlm, ohlu)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}
