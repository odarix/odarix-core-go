package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/common"
)

// ManagerCtor - func-constructor for Manager.
type ManagerCtor func(
	ctx context.Context,
	dialers []Dialer,
	encoderCtor ManagerEncoderCtor,
	refillCtor ManagerRefillCtor,
	shardsNumberPower uint8,
	refillInterval time.Duration,
	errorHandler ErrorHandler,
	clock clockwork.Clock,
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
) (ManagerRefillSender, error)

// ManagerKeeperConfig - config for ManagerKeeper.
type ManagerKeeperConfig struct {
	RotateInterval      time.Duration
	RefillInterval      time.Duration
	RefillSenderManager *RefillSendManagerConfig
}

// ManagerKeeper - a global object through which all writing and sending of data takes place.
type ManagerKeeper struct {
	cfg                *ManagerKeeperConfig
	manager            *Manager
	managerCtor        ManagerCtor
	managerEncoderCtor ManagerEncoderCtor
	managerRefillCtor  ManagerRefillCtor
	mangerRefillSender ManagerRefillSender
	clock              clockwork.Clock
	rwm                *sync.RWMutex
	dialers            []Dialer
	errorHandler       ErrorHandler
	rotateTick         time.Duration
	ctx                context.Context
	stop               chan struct{}
	done               chan struct{}
}

// NewManagerKeeper - init new DeliveryKeeper.
func NewManagerKeeper(
	ctx context.Context,
	cfg *ManagerKeeperConfig,
	managerCtor ManagerCtor,
	managerEncoderCtor ManagerEncoderCtor,
	managerRefillCtor ManagerRefillCtor,
	mangerRefillSenderCtor MangerRefillSenderCtor,
	clock clockwork.Clock,
	dialers []Dialer,
	errorHandler ErrorHandler,
) (*ManagerKeeper, error) {
	var err error
	dk := &ManagerKeeper{
		cfg:                cfg,
		managerCtor:        managerCtor,
		managerEncoderCtor: managerEncoderCtor,
		managerRefillCtor:  managerRefillCtor,
		clock:              clock,
		rwm:                new(sync.RWMutex),
		dialers:            dialers,
		errorHandler:       errorHandler,
		rotateTick:         cfg.RotateInterval,
		ctx:                ctx,
		stop:               make(chan struct{}),
		done:               make(chan struct{}),
	}

	dk.manager, err = dk.managerCtor(
		dk.ctx,
		dk.dialers,
		dk.managerEncoderCtor,
		dk.managerRefillCtor,
		DefaultShardsNumberPower,
		dk.cfg.RefillInterval,
		dk.errorHandler,
		dk.clock,
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
	ticker := dk.clock.NewTicker(dk.rotateTick)
	defer ticker.Stop()
	defer close(dk.done)

	for {
		select {
		case <-ticker.Chan():
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
				dk.managerEncoderCtor,
				dk.managerRefillCtor,
				prevManager.CalculateRequiredShardsNumberPower(),
				dk.cfg.RefillInterval,
				dk.errorHandler,
				dk.clock,
			)
			if err != nil {
				dk.errorHandler("fail create manager", err)
				dk.rwm.Unlock()
				continue
			}
			dk.manager = newManager
			dk.rwm.Unlock()
			// TODO need to pick a reasonable time
			shutdownCtx, cancel := context.WithTimeout(dk.ctx, 15*time.Second)
			if err := prevManager.Shutdown(shutdownCtx); err != nil {
				dk.errorHandler("fail shutdown manager", err)
			}
			cancel()
			dk.manager.Open(dk.ctx)
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
func (dk *ManagerKeeper) Send(ctx context.Context, data common.ShardedData) (bool, error) {
	dk.rwm.RLock()
	defer dk.rwm.RUnlock()
	select {
	case <-dk.stop:
		return false, ErrShutdown
	default:
	}

	return dk.manager.Send(ctx, data)
}

// Shutdown - stop ticker and waits until Manager end to work and then exits.
func (dk *ManagerKeeper) Shutdown(ctx context.Context) error {
	close(dk.stop)
	<-dk.done

	dk.rwm.Lock()
	if err := dk.manager.Close(); err != nil {
		dk.rwm.Unlock()
		return err
	}

	if err := dk.manager.Shutdown(ctx); err != nil {
		dk.rwm.Unlock()
		return err
	}
	dk.rwm.Unlock()

	return dk.mangerRefillSender.Shutdown(ctx)
}
