package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

// ManagerCtor - func-constructor for Manager.
type ManagerCtor func(
	ctx context.Context,
	dialers []Dialer,
	hashdexCtor HashdexCtor,
	encoderCtor ManagerEncoderCtor,
	refillCtor ManagerRefillCtor,
	shardsNumberPower uint8,
	refillInterval time.Duration,
	rejectNotifyer RejectNotifyer,
	haTracker HATracker,
	errorHandler ErrorHandler,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) (*Manager, error)

// ManagerRefillSender - interface for refill Send manger.
type ManagerRefillSender interface {
	Run(context.Context)
	Shutdown(ctx context.Context) error
}

// MangerRefillSenderCtor - func-constructor for MangerRefillSender.
type MangerRefillSenderCtor func(
	*RefillSendManagerConfig,
	[]Dialer,
	ErrorHandler,
	clockwork.Clock,
	prometheus.Registerer,
) (ManagerRefillSender, error)

// ManagerKeeperConfig - config for ManagerKeeper.
type ManagerKeeperConfig struct {
	RotateInterval       time.Duration
	RefillInterval       time.Duration
	RejectRotateInterval time.Duration
	ShutdownTimeout      time.Duration
	RefillSenderManager  *RefillSendManagerConfig
}

// ManagerKeeper - a global object through which all writing and sending of data takes place.
type ManagerKeeper struct {
	cfg                *ManagerKeeperConfig
	manager            *Manager
	managerCtor        ManagerCtor
	hashdexCtor        HashdexCtor
	managerEncoderCtor ManagerEncoderCtor
	managerRefillCtor  ManagerRefillCtor
	mangerRefillSender ManagerRefillSender
	clock              clockwork.Clock
	rwm                *sync.RWMutex
	dialers            []Dialer
	haTracker          HATracker
	errorHandler       ErrorHandler
	rotateTimer        *RotateTimer
	ctx                context.Context
	stop               chan struct{}
	done               chan struct{}
	registerer         prometheus.Registerer
	// stat
	sendDuration *prometheus.HistogramVec
	inFlight     prometheus.Gauge
}

// NewManagerKeeper - init new DeliveryKeeper.
//
//revive:disable-next-line:function-length long but readable
func NewManagerKeeper(
	ctx context.Context,
	cfg *ManagerKeeperConfig,
	managerCtor ManagerCtor,
	hashdexCtor HashdexCtor,
	managerEncoderCtor ManagerEncoderCtor,
	managerRefillCtor ManagerRefillCtor,
	mangerRefillSenderCtor MangerRefillSenderCtor,
	clock clockwork.Clock,
	dialers []Dialer,
	errorHandler ErrorHandler,
	registerer prometheus.Registerer,
) (*ManagerKeeper, error) {
	var err error
	factory := NewConflictRegisterer(registerer)
	haTracker := NewHighAvailabilityTracker(ctx, registerer, clock)
	dk := &ManagerKeeper{
		cfg:                cfg,
		managerCtor:        managerCtor,
		hashdexCtor:        hashdexCtor,
		managerEncoderCtor: managerEncoderCtor,
		managerRefillCtor:  managerRefillCtor,
		clock:              clock,
		rwm:                new(sync.RWMutex),
		dialers:            dialers,
		haTracker:          haTracker,
		errorHandler:       errorHandler,
		rotateTimer:        NewRotateTimer(clock, cfg.RotateInterval, cfg.RejectRotateInterval),
		ctx:                ctx,
		stop:               make(chan struct{}),
		done:               make(chan struct{}),
		registerer:         registerer,
		sendDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_keeper_send_duration_seconds",
				Help:    "Duration of sending data(s).",
				Buckets: prometheus.ExponentialBucketsRange(0.1, 20, 10),
			},
			[]string{"state"},
		),
		inFlight: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "odarix_core_delivery_manager_keeper_in_flight",
				Help: "The number of requests being processed.",
			},
		),
	}

	dk.manager, err = dk.managerCtor(
		dk.ctx,
		dk.dialers,
		dk.hashdexCtor,
		dk.managerEncoderCtor,
		dk.managerRefillCtor,
		DefaultShardsNumberPower,
		dk.cfg.RefillInterval,
		dk.rotateTimer,
		dk.haTracker,
		dk.errorHandler,
		dk.clock,
		dk.registerer,
	)
	if err != nil {
		return nil, err
	}
	dk.manager.Open(ctx)

	dk.mangerRefillSender, err = mangerRefillSenderCtor(
		cfg.RefillSenderManager,
		dialers,
		errorHandler,
		clock,
		dk.registerer,
	)
	if err != nil {
		return nil, err
	}

	go dk.mangerRefillSender.Run(ctx)
	go dk.rotateLoop(ctx)

	return dk, nil
}

// rotateLoop - loop for rotate Manage.
func (dk *ManagerKeeper) rotateLoop(ctx context.Context) {
	defer dk.rotateTimer.Stop()
	defer close(dk.done)

	for {
		select {
		case <-dk.rotateTimer.Chan():
			dk.rwm.Lock()
			prevManager := dk.manager
			if err := prevManager.Close(); err != nil {
				dk.errorHandler("fail close manager", err)
				dk.rwm.Unlock()
				continue
			}
			newManager, err := dk.managerCtor(
				dk.ctx,
				dk.dialers,
				dk.hashdexCtor,
				dk.managerEncoderCtor,
				dk.managerRefillCtor,
				prevManager.CalculateRequiredShardsNumberPower(),
				dk.cfg.RefillInterval,
				dk.rotateTimer,
				dk.haTracker,
				dk.errorHandler,
				dk.clock,
				dk.registerer,
			)
			if err != nil {
				dk.errorHandler("fail create manager", err)
				dk.rwm.Unlock()
				continue
			}
			dk.manager = newManager
			dk.rwm.Unlock()
			shutdownCtx, cancel := context.WithTimeout(dk.ctx, dk.cfg.ShutdownTimeout)
			if err := prevManager.Shutdown(shutdownCtx); err != nil {
				dk.errorHandler("fail shutdown manager", err)
			}
			cancel()
			dk.manager.Open(dk.ctx)
			dk.rotateTimer.Reset()
		case <-dk.stop:
			return
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), ErrShutdown) {
				dk.errorHandler("rotate loop context canceled", context.Cause(ctx))
			}
			return
		}
	}
}

// Send - send metrics data to encode and send.
func (dk *ManagerKeeper) Send(ctx context.Context, data ProtoData) (bool, error) {
	dk.inFlight.Inc()
	defer dk.inFlight.Dec()
	start := time.Now()

	dk.rwm.RLock()
	defer dk.rwm.RUnlock()
	select {
	case <-dk.stop:
		return false, ErrShutdown
	default:
	}
	delivered, err := dk.manager.Send(ctx, data)
	if err != nil {
		dk.sendDuration.With(prometheus.Labels{"state": "error"}).Observe(time.Since(start).Seconds())
		return delivered, err
	}
	if !delivered {
		dk.sendDuration.With(prometheus.Labels{"state": "refill"}).Observe(time.Since(start).Seconds())
		return delivered, err
	}
	dk.sendDuration.With(prometheus.Labels{"state": "success"}).Observe(time.Since(start).Seconds())
	return delivered, nil
}

// Shutdown - stop ticker and waits until Manager end to work and then exits.
func (dk *ManagerKeeper) Shutdown(ctx context.Context) error {
	defer dk.haTracker.Destroy()
	close(dk.stop)
	<-dk.done

	var errs error
	dk.rwm.RLock()
	errs = multierr.Append(errs, dk.manager.Shutdown(ctx))
	dk.rwm.RUnlock()

	return multierr.Append(errs, dk.mangerRefillSender.Shutdown(ctx))
}

// RotateTimer - custom timer with reset the timer for the delay time.
type RotateTimer struct {
	clock            clockwork.Clock
	timer            clockwork.Timer
	rotateAt         time.Time
	delayAfterNotify time.Duration
	durationBlock    time.Duration
	mx               *sync.Mutex
}

// NewRotateTimer - init new RotateTimer. The duration durationBlock and delayAfterNotify must be greater than zero;
// if not, Ticker will panic. Stop the ticker to release associated resources.
func NewRotateTimer(clock clockwork.Clock, durationBlock, delayAfterNotify time.Duration) *RotateTimer {
	return &RotateTimer{
		clock:            clock,
		timer:            clock.NewTimer(durationBlock),
		rotateAt:         clock.Now().Add(durationBlock),
		delayAfterNotify: delayAfterNotify,
		durationBlock:    durationBlock,
		mx:               new(sync.Mutex),
	}
}

// NotifyOnReject - reset the timer for the delay time if the time does not exceed the duration Block.
func (rt *RotateTimer) NotifyOnReject() {
	rt.mx.Lock()
	defer rt.mx.Unlock()
	until := rt.rotateAt.Sub(rt.clock.Now())
	if rt.delayAfterNotify > until {
		if until > 0 {
			rt.timer.Reset(until)
		}
		return
	}
	rt.timer.Reset(rt.delayAfterNotify)
}

// Chan - return chan with ticker time.
func (rt *RotateTimer) Chan() <-chan time.Time {
	return rt.timer.Chan()
}

// Reset - changes the timer to expire after duration Block.
func (rt *RotateTimer) Reset() {
	rt.mx.Lock()
	rt.rotateAt = rt.clock.Now().Add(rt.durationBlock)
	rt.timer.Reset(rt.durationBlock)
	rt.mx.Unlock()
}

// Stop - prevents the Timer from firing.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (rt *RotateTimer) Stop() {
	if !rt.timer.Stop() {
		<-rt.timer.Chan()
	}
}
