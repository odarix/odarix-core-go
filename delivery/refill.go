package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/common"
	"go.uber.org/multierr"
)

var (
	// ErrSnapshotNotFoundRefill - error snapshot not found in refill.
	ErrSnapshotNotFoundRefill = errors.New("snapshot not found")
	// ErrSnapshotRequired - error for case if not found snapshots or prevous segment.
	ErrSnapshotRequired = errors.New("snapshot is required")
	// ErrShardsNotEqual - error when number of shards not equal.
	ErrShardsNotEqual = errors.New("number of shards not equal")
	// ErrDestinationsNamesNotEqual - error when destinations names not equal.
	ErrDestinationsNamesNotEqual = errors.New("destinations names not equal")
)

// Refill - manager for refills.
type Refill struct {
	sm            *StorageManager
	mx            *sync.RWMutex
	isContinuable bool
}

var _ ManagerRefill = (*Refill)(nil)

// NewRefill - init new RefillManager.
func NewRefill(
	cfg *FileStorageConfig,
	shardsNumberPower uint8,
	blockID uuid.UUID,
	names ...string,
) (*Refill, error) {
	var err error
	rm := &Refill{
		mx: new(sync.RWMutex),
	}

	rm.sm, err = NewStorageManager(cfg, shardsNumberPower, blockID, names...)
	switch err {
	case nil:
		rm.isContinuable = true
	case ErrShardsNotEqual:
		rm.isContinuable = false
	case ErrDestinationsNamesNotEqual:
		rm.isContinuable = false
	default:
		return nil, err
	}

	if !rm.sm.CheckSegmentsSent() {
		rm.isContinuable = false
	}

	return rm, nil
}

// IsContinuable - is it possible to continue the file.
func (rl *Refill) IsContinuable() bool {
	return rl.isContinuable
}

// BlockID - return if exist blockID or nil.
func (rl *Refill) BlockID() uuid.UUID {
	return rl.sm.BlockID()
}

// Shards - return number of Shards.
func (rl *Refill) Shards() int {
	return rl.sm.Shards()
}

// Destinations - return number of Destinations.
func (rl *Refill) Destinations() int {
	return rl.sm.Destinations()
}

// LastSegment - return last ack segment by shard and destination.
func (rl *Refill) LastSegment(shardID uint16, dest string) uint32 {
	return rl.sm.LastSegment(shardID, dest)
}

// Get - get segment from file.
func (rl *Refill) Get(ctx context.Context, key common.SegmentKey) (Segment, error) {
	rl.mx.RLock()
	defer rl.mx.RUnlock()

	return rl.sm.GetSegment(ctx, key)
}

// Ack - increment status by destination and shard if segment is next for current value.
func (rl *Refill) Ack(segKey common.SegmentKey, dest string) {
	rl.sm.Ack(segKey, dest)
}

// Reject - accumulates rejects and serializes and writes to refill while recording statuses.
func (rl *Refill) Reject(segKey common.SegmentKey, dest string) {
	rl.sm.Reject(segKey, dest)
}

// Restore - return snapshot and segments.
func (rl *Refill) Restore(ctx context.Context, key common.SegmentKey) (Snapshot, []Segment, error) {
	rl.mx.RLock()
	defer rl.mx.RUnlock()

	segments := make([]Segment, 0)

	snapshot, err := rl.sm.GetSnapshot(ctx, key)
	if err == nil {
		return snapshot, segments, nil
	}
	if err != ErrSnapshotNotFoundRefill {
		return nil, nil, err
	}

	for {
		seg, err := rl.sm.GetSegment(ctx, key)
		if err != nil {
			return nil, nil, err
		}

		segments = append(
			segments,
			seg,
		)

		snapshot, err := rl.sm.GetSnapshot(ctx, key)
		if err == nil {
			return snapshot, segments, nil
		}
		if err != ErrSnapshotNotFoundRefill {
			return nil, nil, err
		}

		key.Segment--
	}
}

// WriteSegment - write Segment in file.
func (rl *Refill) WriteSegment(ctx context.Context, key common.SegmentKey, seg Segment) error {
	rl.mx.Lock()
	defer rl.mx.Unlock()

	return rl.sm.WriteSegment(ctx, key, seg)
}

// WriteSnapshot - write Snapshot in file.
func (rl *Refill) WriteSnapshot(ctx context.Context, segKey common.SegmentKey, snapshot Snapshot) error {
	rl.mx.Lock()
	defer rl.mx.Unlock()

	return rl.sm.WriteSnapshot(ctx, segKey, snapshot)
}

// WriteAckStatus - write AckStatus.
func (rl *Refill) WriteAckStatus(ctx context.Context) error {
	rl.mx.Lock()
	defer rl.mx.Unlock()

	if err := rl.sm.WriteAckStatus(ctx); err != nil {
		return err
	}

	if !rl.sm.CheckSegmentsSent() {
		return nil
	}

	return rl.sm.DeleteCurrentFile()
}

// IntermediateRename - rename the current file to blockID with temporary extension for further conversion to refill.
//
// Use Rotate nor IntermediateRename + Shutdown. Not both.
func (rl *Refill) IntermediateRename() error {
	rl.mx.Lock()
	defer rl.mx.Unlock()

	return rl.sm.IntermediateRename(rl.makeCompleteFileName())
}

// Shutdown - finalize and close refill.
//
// Use Rotate nor IntermediateRename + Shutdown. Not both.
func (rl *Refill) Shutdown(_ context.Context) error {
	rl.mx.Lock()
	defer rl.mx.Unlock()
	name, ok := rl.sm.GetIntermediateName()
	if !ok {
		name = rl.makeCompleteFileName()
	}

	return multierr.Append(rl.sm.Rename(name), rl.sm.Close())
}

func (rl *Refill) makeCompleteFileName() string {
	return fmt.Sprintf("%013d_%s", time.Now().UnixMilli(), rl.sm.BlockID().String())
}
