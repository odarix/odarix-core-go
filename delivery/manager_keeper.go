package delivery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

const (
	// DefaultRefillInterval - default refill interval.
	DefaultRefillInterval = 10 * time.Second
	// DefaultShutdownTimeout - default shutdown timeout.
	DefaultShutdownTimeout = 15 * time.Second
	// DefaultWorkingDir - default working dir.
	DefaultWorkingDir = "/usr/local/odarix-core"
	// DefaultShardsNumberPower - default shards number power.
	DefaultShardsNumberPower = 0
	// DefaultDesiredBlockSizeBytes - default desired block size bytes.
	DefaultDesiredBlockSizeBytes = 64 << 20
	// DefaultDesiredBlockFormationDuration - default desired block formation duration.
	DefaultDesiredBlockFormationDuration = 2 * time.Hour
	// DefaultBlockSizePercentThresholdForDownscaling - default block size percent threshold for downscaling.
	DefaultBlockSizePercentThresholdForDownscaling = 10
	// DefaultDelayAfterNotify - default delay after notify reject.
	DefaultDelayAfterNotify = 300 * time.Second
	// magic byte for header
	magicByte byte = 189
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
	workingDir string,
	limits Limits,
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
	RefillSendManagerConfig,
	string,
	[]Dialer,
	ErrorHandler,
	clockwork.Clock,
	prometheus.Registerer,
) (ManagerRefillSender, error)

// ManagerKeeperConfig - config for ManagerKeeper.
//
// Block - config for block.
// RefillSenderManager - config for refill sender manager.
// ShutdownTimeout - timeout to cancel context on shutdown.
// RefillInterval - interval for holding a segment in memory before sending it to refill.
type ManagerKeeperConfig struct {
	RefillSenderManager RefillSendManagerConfig
	WorkingDir          string        `validate:"required"`
	ShutdownTimeout     time.Duration `validate:"gtefield=RefillInterval,required"`
	RefillInterval      time.Duration `validate:"ltefield=ShutdownTimeout,required"`
}

// DefaultManagerKeeperConfig - generate default ManagerKeeperConfig.
func DefaultManagerKeeperConfig() ManagerKeeperConfig {
	return ManagerKeeperConfig{
		RefillSenderManager: DefaultRefillSendManagerConfig(),
		WorkingDir:          DefaultWorkingDir,
		ShutdownTimeout:     DefaultShutdownTimeout,
		RefillInterval:      DefaultRefillInterval,
	}
}

// Validate - check the config for correct parameters.
func (c *ManagerKeeperConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

// ManagerKeeper - a global object through which all writing and sending of data takes place.
type ManagerKeeper struct {
	cfg                ManagerKeeperConfig
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
	currentState       *CurrentState
	autosharder        *Autosharder
	ctx                context.Context
	limitTrigger       chan struct{}
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
	cfg ManagerKeeperConfig,
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
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	factory := NewConflictRegisterer(registerer)
	haTracker := NewHighAvailabilityTracker(ctx, registerer, clock)
	cs := NewCurrentState(cfg.WorkingDir)
	if err = cs.Read(); err != nil {
		errorHandler("fail read current state", err)
	}
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
		rotateTimer:        NewRotateTimer(clock, cs.Block()),
		currentState:       cs,
		ctx:                ctx,
		limitTrigger:       make(chan struct{}),
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

	snp := cs.ShardsNumberPower()
	dk.manager, err = dk.managerCtor(
		dk.ctx,
		dk.dialers,
		dk.hashdexCtor,
		dk.managerEncoderCtor,
		dk.managerRefillCtor,
		snp,
		cfg.RefillInterval,
		cfg.WorkingDir,
		cs.Limits(),
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
		cfg.WorkingDir,
		dialers,
		errorHandler,
		clock,
		dk.registerer,
	)
	if err != nil {
		return nil, err
	}

	dk.autosharder = NewAutosharder(clock, cs.Block(), snp)
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
			if err := dk.rotate(); err != nil {
				dk.errorHandler("failed rotate block", err)
			}
			dk.drainedTrigger()
			dk.rotateTimer.Reset()
		case <-dk.limitTrigger:
			if err := dk.rotate(); err != nil {
				dk.errorHandler("failed rotate block", err)
			}
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

func (dk *ManagerKeeper) rotate() error {
	dk.rwm.Lock()
	prevManager := dk.manager
	if err := prevManager.Close(); err != nil {
		dk.rwm.Unlock()
		return fmt.Errorf("fail close manager: %w", err)
	}
	snp := dk.autosharder.ShardsNumberPower(prevManager.MaxBlockBytes())
	limits := prevManager.Limits()
	newManager, err := dk.managerCtor(
		dk.ctx,
		dk.dialers,
		dk.hashdexCtor,
		dk.managerEncoderCtor,
		dk.managerRefillCtor,
		snp,
		dk.cfg.RefillInterval,
		dk.cfg.WorkingDir,
		limits,
		dk.rotateTimer,
		dk.haTracker,
		dk.errorHandler,
		dk.clock,
		dk.registerer,
	)
	if err != nil {
		dk.rwm.Unlock()
		return fmt.Errorf("fail create manager: %w", err)
	}
	dk.autosharder.Reset(dk.currentState.Block())
	if err = dk.currentState.Write(snp, &limits); err != nil {
		dk.errorHandler("fail write current state", err)
	}
	dk.manager = newManager
	dk.rwm.Unlock()
	shutdownCtx, cancel := context.WithTimeout(dk.ctx, dk.cfg.ShutdownTimeout)
	if err := prevManager.Shutdown(shutdownCtx); err != nil {
		dk.errorHandler("fail shutdown manager", err)
	}
	cancel()
	dk.manager.Open(dk.ctx)
	return nil
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
	if dk.manager.MaxBlockBytes() >= dk.currentState.Block().DesiredBlockSizeBytes {
		dk.notifyOnLimits()
	}
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

// SendOpenHead - send metrics data to encode and send.
func (dk *ManagerKeeper) SendOpenHead(ctx context.Context, data ProtoData) (bool, error) {
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
	delivered, err := dk.manager.SendOpenHead(ctx, data)
	if dk.manager.MaxBlockBytes() >= dk.currentState.Block().DesiredBlockSizeBytes {
		dk.notifyOnLimits()
	}
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

// notifyOnLimits - triggers a notification regardless of the remaining block time.
func (dk *ManagerKeeper) notifyOnLimits() {
	select {
	case dk.limitTrigger <- struct{}{}:
	default:
	}
}

// drainedTrigger - drained trigger channel.
func (dk *ManagerKeeper) drainedTrigger() {
	select {
	case <-dk.limitTrigger:
	default:
	}
}

// Shutdown - stop ticker and waits until Manager end to work and then exits.
func (dk *ManagerKeeper) Shutdown(ctx context.Context) error {
	defer dk.haTracker.Destroy()
	close(dk.stop)
	<-dk.done
	dk.drainedTrigger()
	close(dk.limitTrigger)

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
func NewRotateTimer(clock clockwork.Clock, cfg BlockLimits) *RotateTimer {
	rt := &RotateTimer{
		clock:            clock,
		timer:            clock.NewTimer(cfg.DesiredBlockFormationDuration),
		rotateAt:         clock.Now().Add(cfg.DesiredBlockFormationDuration),
		delayAfterNotify: cfg.DelayAfterNotify,
		durationBlock:    cfg.DesiredBlockFormationDuration,
		mx:               new(sync.Mutex),
	}

	return rt
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

// Reset - changes the timer to expire after duration Block and clearing channels.
func (rt *RotateTimer) Reset() {
	rt.mx.Lock()
	rt.rotateAt = rt.clock.Now().Add(rt.durationBlock)
	if !rt.timer.Stop() {
		select {
		case <-rt.timer.Chan():
		default:
		}
	}
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

// BlockLimits - config for Autosharder and Rotatetimer.
//
// DesiredBlockSizeBytes - desired block size, default 64Mb, not guaranteed, may be slightly exceeded.
// DesiredBlockFormationDuration - the desired length of shaping time,
// 2 hours by default, is not guaranteed, it can be slightly exceeded.
// DelayAfterNotify - delay after reject notification, must be greater than zero.
// BlockSizePercentThresholdForDownscaling - Block size threshold when scaling down, 10% by default.
type BlockLimits struct {
	DesiredBlockSizeBytes                   int64         `validate:"required"`
	BlockSizePercentThresholdForDownscaling int64         `validate:"max=100,min=0"`
	DesiredBlockFormationDuration           time.Duration `validate:"required"`
	DelayAfterNotify                        time.Duration `validate:"required"`
}

// DefaultBlockLimits - generate default BlockLimits.
func DefaultBlockLimits() BlockLimits {
	return BlockLimits{
		DesiredBlockSizeBytes:                   DefaultDesiredBlockSizeBytes,
		BlockSizePercentThresholdForDownscaling: DefaultBlockSizePercentThresholdForDownscaling,
		DesiredBlockFormationDuration:           DefaultDesiredBlockFormationDuration,
		DelayAfterNotify:                        DefaultDelayAfterNotify,
	}
}

// MarshalBinary - encoding to byte.
func (l *BlockLimits) MarshalBinary() ([]byte, error) {
	//revive:disable-next-line:add-constant sum 3*8+1(BlockSizePercentThresholdForDownscaling max 100)
	buf := make([]byte, 0, 25)

	buf = binary.AppendUvarint(buf, uint64(l.DesiredBlockSizeBytes))
	buf = binary.AppendUvarint(buf, uint64(l.BlockSizePercentThresholdForDownscaling))
	buf = binary.AppendUvarint(buf, uint64(l.DesiredBlockFormationDuration))
	buf = binary.AppendUvarint(buf, uint64(l.DelayAfterNotify))
	return buf, nil
}

// UnmarshalBinary - decoding from byte.
func (l *BlockLimits) UnmarshalBinary(data []byte) error {
	var offset int

	desiredBlockSizeBytes, n := binary.Uvarint(data[offset:])
	l.DesiredBlockSizeBytes = int64(desiredBlockSizeBytes)
	offset += n

	blockSizePercentThresholdForDownscaling, n := binary.Uvarint(data[offset:])
	l.BlockSizePercentThresholdForDownscaling = int64(blockSizePercentThresholdForDownscaling)
	offset += n

	desiredBlockFormationDuration, n := binary.Uvarint(data[offset:])
	l.DesiredBlockFormationDuration = time.Duration(desiredBlockFormationDuration)
	offset += n

	delayAfterNotify, _ := binary.Uvarint(data[offset:])
	l.DelayAfterNotify = time.Duration(delayAfterNotify)

	return nil
}

// Limits - all limits for work.
type Limits struct {
	OpenHead OpenHeadLimits
	Block    BlockLimits
	Hashdex  common.HashdexLimits
}

// DefaultLimits - generate default Limits.
func DefaultLimits() Limits {
	return Limits{
		OpenHead: DefaultOpenHeadLimits(),
		Block:    DefaultBlockLimits(),
		Hashdex:  common.DefaultHashdexLimits(),
	}
}

// CurrentState - current state data.
type CurrentState struct {
	shardsNumberPower *uint8
	limits            *Limits
	dir               string
	fileName          string
	validate          *validator.Validate
}

// NewCurrentState - init new CurrentState.
func NewCurrentState(dir string) *CurrentState {
	return &CurrentState{
		dir:      dir,
		fileName: "state.db",
		validate: validator.New(),
	}
}

// ShardsNumberPower - return if exist value ShardsNumberPower or default.
func (cs *CurrentState) ShardsNumberPower() uint8 {
	if cs.shardsNumberPower == nil {
		return DefaultShardsNumberPower
	}
	return *cs.shardsNumberPower
}

// Limits - return current all limits.
func (cs *CurrentState) Limits() Limits {
	if cs.limits == nil {
		return DefaultLimits()
	}
	if err := cs.validate.Struct(cs.limits); err != nil {
		return DefaultLimits()
	}
	return *cs.limits
}

// Block - return current BlockLimits.
func (cs *CurrentState) Block() BlockLimits {
	if cs.limits == nil {
		return DefaultBlockLimits()
	}
	if err := cs.validate.Struct(cs.limits.Block); err != nil {
		return DefaultBlockLimits()
	}
	return cs.limits.Block
}

// Read - read current state data from file.
func (cs *CurrentState) Read() error {
	b, err := os.ReadFile(filepath.Join(cs.dir, cs.fileName))
	if err != nil {
		return fmt.Errorf("read file %s: %w", cs.fileName, err)
	}

	if err = cs.unmarshal(b); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	return nil
}

// unmarshal - decoding from byte.
//
//revive:disable-next-line:function-length long but readable
//revive:disable-next-line:cyclomatic  but readable
func (cs *CurrentState) unmarshal(data []byte) error {
	var offset int

	mb, n := binary.Uvarint(data[offset:])
	if byte(mb) != magicByte {
		return fmt.Errorf("%w: file dont have magic byte: %d", ErrCorruptedFile, mb)
	}
	offset += n

	// read shards number power with checksum
	chksm, n := binary.Uvarint(data[offset:])
	offset += n
	length, n := binary.Uvarint(data[offset:])
	offset += n
	if uint32(chksm) != crc32.ChecksumIEEE(data[offset:offset+int(length)]) {
		return fmt.Errorf("%w: check sum not equal for shards number power", ErrCorruptedFile)
	}
	usnp, n := binary.Uvarint(data[offset : offset+int(length)])
	snp := uint8(usnp)
	cs.shardsNumberPower = &snp
	offset += n

	if cs.limits == nil {
		cs.limits = new(Limits)
	}

	// read open head limits with checksum
	chksm, n = binary.Uvarint(data[offset:])
	offset += n
	length, n = binary.Uvarint(data[offset:])
	offset += n
	if uint32(chksm) != crc32.ChecksumIEEE(data[offset:offset+int(length)]) {
		return fmt.Errorf("%w: check sum not equal for open head limits", ErrCorruptedFile)
	}
	if err := cs.limits.OpenHead.UnmarshalBinary(data[offset : offset+int(length)]); err != nil {
		return err
	}
	offset += int(length)

	// read block limits with checksum
	chksm, n = binary.Uvarint(data[offset:])
	offset += n
	length, n = binary.Uvarint(data[offset:])
	offset += n
	if uint32(chksm) != crc32.ChecksumIEEE(data[offset:offset+int(length)]) {
		return fmt.Errorf("%w: check sum not equal for block limits", ErrCorruptedFile)
	}
	if err := cs.limits.Block.UnmarshalBinary(data[offset : offset+int(length)]); err != nil {
		return err
	}
	offset += int(length)

	// read hashdex limits with checksum
	chksm, n = binary.Uvarint(data[offset:])
	offset += n
	length, n = binary.Uvarint(data[offset:])
	offset += n
	if uint32(chksm) != crc32.ChecksumIEEE(data[offset:offset+int(length)]) {
		return fmt.Errorf("%w: check sum not equal for hashdex limits", ErrCorruptedFile)
	}
	if err := cs.limits.Hashdex.UnmarshalBinary(data[offset : offset+int(length)]); err != nil {
		return err
	}
	offset += int(length)

	// read checksum file
	chksm, _ = binary.Uvarint(data[offset:])
	if uint32(chksm) != crc32.ChecksumIEEE(data[:len(data)-5]) {
		return fmt.Errorf("%w: check sum not equal", ErrCorruptedFile)
	}
	return nil
}

// Write - write current state data in file.
func (cs *CurrentState) Write(snp uint8, limits *Limits) error {
	cs.shardsNumberPower = &snp
	cs.limits = limits
	out, err := cs.marshal()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	//revive:disable-next-line:add-constant file permissions simple readable as octa-number
	if err = os.WriteFile(filepath.Join(cs.dir, cs.fileName), out, 0o600); err != nil {
		return fmt.Errorf("write file %s: %w", cs.fileName, err)
	}

	return nil
}

// marshal - encoding to byte.
func (cs *CurrentState) marshal() ([]byte, error) {
	snpb := binary.AppendUvarint([]byte{}, uint64(cs.ShardsNumberPower()))

	ohlb, err := cs.limits.OpenHead.MarshalBinary()
	if err != nil {
		return nil, err
	}

	blb, err := cs.limits.Block.MarshalBinary()
	if err != nil {
		return nil, err
	}

	hlb, err := cs.limits.Hashdex.MarshalBinary()
	if err != nil {
		return nil, err
	}

	//revive:disable-next-line:add-constant 2 magicByte + 1 (shardsNumberPower) + max size len 1 byte + 5*4(Checksum)
	bufSize := 32 + len(ohlb) + len(blb) + len(hlb)
	buf := make([]byte, 0, bufSize)
	buf = binary.AppendUvarint(buf, uint64(magicByte))

	// write shards number power with checksum
	buf = binary.AppendUvarint(buf, uint64(crc32.ChecksumIEEE(snpb)))
	buf = binary.AppendUvarint(buf, uint64(len(snpb)))
	buf = append(buf, snpb...)

	// write open head limits with checksum
	buf = binary.AppendUvarint(buf, uint64(crc32.ChecksumIEEE(ohlb)))
	buf = binary.AppendUvarint(buf, uint64(len(ohlb)))
	buf = append(buf, ohlb...)

	// write block limits with checksum
	buf = binary.AppendUvarint(buf, uint64(crc32.ChecksumIEEE(blb)))
	buf = binary.AppendUvarint(buf, uint64(len(blb)))
	buf = append(buf, blb...)

	// write hashdex limits with checksum
	buf = binary.AppendUvarint(buf, uint64(crc32.ChecksumIEEE(hlb)))
	buf = binary.AppendUvarint(buf, uint64(len(hlb)))
	buf = append(buf, hlb...)

	buf = binary.AppendUvarint(buf, uint64(crc32.ChecksumIEEE(buf)))

	return buf, nil
}

// Autosharder - selects a new value for the number of shards
// based on the achievement of time or block size limits.
type Autosharder struct {
	clock                    clockwork.Clock
	start                    time.Time
	cfg                      BlockLimits
	currentShardsNumberPower uint8
}

// NewAutosharder - init new Autosharder.
func NewAutosharder(
	clock clockwork.Clock,
	cfg BlockLimits,
	shardsNumberPower uint8,
) *Autosharder {
	return &Autosharder{
		clock:                    clock,
		start:                    clock.Now(),
		cfg:                      cfg,
		currentShardsNumberPower: shardsNumberPower,
	}
}

// ShardsNumberPower - calculate new shards number of power.
func (as *Autosharder) ShardsNumberPower(blockBytes int64) uint8 {
	switch {
	case blockBytes > as.cfg.DesiredBlockSizeBytes:
		as.currentShardsNumberPower = as.calculateByTime()
		return as.currentShardsNumberPower
	case as.clock.Since(as.start).Seconds() >= as.cfg.DesiredBlockFormationDuration.Seconds():
		//revive:disable-next-line:add-constant 100% not ned constant
		size := float64((blockBytes * (100 + as.cfg.BlockSizePercentThresholdForDownscaling)) / 100)
		as.currentShardsNumberPower = as.calculateBySize(size)
		return as.currentShardsNumberPower
	default:
		return as.currentShardsNumberPower
	}
}

// calculateByTime - calculate by elapsed time.
func (as *Autosharder) calculateByTime() uint8 {
	k := as.clock.Since(as.start).Seconds() / as.cfg.DesiredBlockFormationDuration.Seconds()
	//revive:disable:add-constant edge values is more readable without constants
	switch {
	case k > 1:
		return as.currentShardsNumberPower
	case k > 0.5:
		return as.incShardsNumberPower(1)
	case k > 0.25:
		return as.incShardsNumberPower(2)
	case k > 0.125:
		return as.incShardsNumberPower(3)
	case k > 0.0625:
		return as.incShardsNumberPower(4)
	default:
		return as.incShardsNumberPower(5)
	}
	//revive:enable
}

// calculateBySize - calculate by elapsed block size.
func (as *Autosharder) calculateBySize(blockBytes float64) uint8 {
	k := blockBytes / float64(as.cfg.DesiredBlockSizeBytes)
	//revive:disable:add-constant edge values is more readable without constants
	switch {
	case k > 0.5:
		return as.currentShardsNumberPower
	case k > 0.25:
		return as.decShardsNumberPower(1)
	case k > 0.125:
		return as.decShardsNumberPower(2)
	case k > 0.0625:
		return as.decShardsNumberPower(3)
	case k > 0.03125:
		return as.decShardsNumberPower(4)
	default:
		return as.decShardsNumberPower(5)
	}
	//revive:enable
}

// incShardsNumberPower - increase the current ShardsNumberPower considering the maximum value.
func (as *Autosharder) incShardsNumberPower(in uint8) uint8 {
	if as.currentShardsNumberPower >= math.MaxUint8-in {
		return math.MaxUint8
	}
	return as.currentShardsNumberPower + in
}

// decShardsNumberPower - decrease the current ShardsNumberPower considering the minimum value
func (as *Autosharder) decShardsNumberPower(in uint8) uint8 {
	if as.currentShardsNumberPower <= in {
		return 0
	}
	return as.currentShardsNumberPower - in
}

// Reset - reset state Autosharder.
func (as *Autosharder) Reset(cfg BlockLimits) {
	as.start = as.clock.Now()
	as.cfg = cfg
}
