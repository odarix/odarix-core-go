package appender

import (
	"context"
	"github.com/odarix/odarix-core-go/relabeler/logger"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/relabeler"
	"github.com/odarix/odarix-core-go/relabeler/config"
)

// Storage - head storage.
type Storage interface {
	Add(head relabeler.Head)
}

// HeadBuilder - head builder.
type HeadBuilder interface {
	Build() (relabeler.Head, error)
	BuildWithConfig(inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16) (relabeler.Head, error)
}

type HeadActivator interface {
	Activate(headID string) error
}

type NoOpHeadActivator struct{}

func (NoOpHeadActivator) Activate(headID string) error { return nil }

// RotatableHead - head wrapper, allows rotations.
type RotatableHead struct {
	head          relabeler.Head
	storage       Storage
	builder       HeadBuilder
	headActivator HeadActivator
}

// ID - relabeler.Head interface implementation.
func (h *RotatableHead) ID() string {
	return h.head.ID()
}

// Generation - relabeler.Head interface implementation.
func (h *RotatableHead) Generation() uint64 {
	return h.head.Generation()
}

// String serialize as string.
func (h *RotatableHead) String() string {
	return h.head.String()
}

// Append - relabeler.Head interface implementation.
func (h *RotatableHead) Append(
	ctx context.Context,
	incomingData *relabeler.IncomingData,
	state *cppbridge.State,
	relabelerID string,
	commitToWal bool,
) ([][]*cppbridge.InnerSeries, cppbridge.RelabelerStats, error) {
	return h.head.Append(ctx, incomingData, state, relabelerID, commitToWal)
}

// RotatableHead - relabeler.Head interface implementation.
func (h *RotatableHead) CommitToWal() error {
	return h.head.CommitToWal()
}

// ForEachShard - relabeler.Head interface implementation.
func (h *RotatableHead) ForEachShard(fn relabeler.ShardFn) error {
	return h.head.ForEachShard(fn)
}

// OnShard - relabeler.Head interface implementation.
func (h *RotatableHead) OnShard(shardID uint16, fn relabeler.ShardFn) error {
	return h.head.OnShard(shardID, fn)
}

// NumberOfShards - relabeler.Head interface implementation.
func (h *RotatableHead) NumberOfShards() uint16 {
	return h.head.NumberOfShards()
}

// Stop - relabeler.Head interface implementation.
func (h *RotatableHead) Stop() {
	h.head.Stop()
}

// Flush - relabeler.Head interface implementation.
func (h *RotatableHead) Flush() error {
	return h.head.Flush()
}

// Reconfigure - relabeler.Head interface implementation.
func (h *RotatableHead) Reconfigure(inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16) error {
	if h.head.NumberOfShards() != numberOfShards {
		return h.RotateWithConfig(inputRelabelerConfigs, numberOfShards)
	}
	return h.head.Reconfigure(inputRelabelerConfigs, numberOfShards)
}

// WriteMetrics - relabeler.Head interface implementation.
func (h *RotatableHead) WriteMetrics() {
	h.head.WriteMetrics()
}

// Status return head stats.
func (h *RotatableHead) Status(limit int) relabeler.HeadStatus {
	return h.head.Status(limit)
}

// Close - relabeler.Head interface implementation.
func (h *RotatableHead) Close() error {
	return h.head.Close()
}

// NewRotatableHead - RotatableHead constructor.
func NewRotatableHead(head relabeler.Head, storage Storage, builder HeadBuilder, headActivator HeadActivator) *RotatableHead {
	return &RotatableHead{
		head:          head,
		storage:       storage,
		builder:       builder,
		headActivator: headActivator,
	}
}

// Rotate - relabeler.Head interface implementation.
func (h *RotatableHead) Rotate() error {
	newHead, err := h.builder.Build()
	if err != nil {
		return err
	}

	if err = h.headActivator.Activate(newHead.ID()); err != nil {
		return err
	}

	if err = h.head.CommitToWal(); err != nil {
		logger.Errorf("failed to commit wal on rotation: %v", err)
	}
	h.head.Stop()

	h.storage.Add(h.head)
	h.head = newHead
	return nil
}

func (h *RotatableHead) RotateWithConfig(inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16) error {
	newHead, err := h.builder.BuildWithConfig(inputRelabelerConfigs, numberOfShards)
	if err != nil {
		return err
	}

	if err = h.headActivator.Activate(newHead.ID()); err != nil {
		return err
	}

	if err = h.head.CommitToWal(); err != nil {
		logger.Errorf("failed to commit wal on rotation: %v", err)
	}
	h.head.Stop()

	h.storage.Add(h.head)
	h.head = newHead
	return nil
}

func (h *RotatableHead) Discard() error {
	return h.head.Discard()
}

type HeapProfileWriter interface {
	WriteHeapProfile() error
}

type HeapProfileWritableHead struct {
	head              relabeler.Head
	heapProfileWriter HeapProfileWriter
}

func (h *HeapProfileWritableHead) ID() string {
	return h.head.ID()
}

func (h *HeapProfileWritableHead) Generation() uint64 {
	return h.head.Generation()
}

// String serialize as string.
func (h *HeapProfileWritableHead) String() string {
	return h.head.String()
}

func (h *HeapProfileWritableHead) Append(
	ctx context.Context,
	incomingData *relabeler.IncomingData,
	state *cppbridge.State,
	relabelerID string,
	commitToWal bool,
) ([][]*cppbridge.InnerSeries, cppbridge.RelabelerStats, error) {
	return h.head.Append(ctx, incomingData, state, relabelerID, commitToWal)
}

func (h *HeapProfileWritableHead) CommitToWal() error {
	return h.head.CommitToWal()
}

func (h *HeapProfileWritableHead) ForEachShard(fn relabeler.ShardFn) error {
	return h.head.ForEachShard(fn)
}

func (h *HeapProfileWritableHead) OnShard(shardID uint16, fn relabeler.ShardFn) error {
	return h.head.OnShard(shardID, fn)
}

func (h *HeapProfileWritableHead) NumberOfShards() uint16 {
	return h.head.NumberOfShards()
}

func (h *HeapProfileWritableHead) Reconfigure(inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16) error {
	return h.head.Reconfigure(inputRelabelerConfigs, numberOfShards)
}

// Stop - relabeler.Head interface implementation.
func (h *HeapProfileWritableHead) Stop() {
	h.head.Stop()
}

// Flush - relabeler.Head interface implementation.
func (h *HeapProfileWritableHead) Flush() error {
	return h.head.Flush()
}

func (h *HeapProfileWritableHead) WriteMetrics() {
	h.head.WriteMetrics()
}

func (h *HeapProfileWritableHead) Status(limit int) relabeler.HeadStatus {
	return h.head.Status(limit)
}

func (h *HeapProfileWritableHead) Rotate() error {
	if err := h.head.Rotate(); err != nil {
		return err
	}

	return h.heapProfileWriter.WriteHeapProfile()
}

func (h *HeapProfileWritableHead) Close() error {
	return h.head.Close()
}

func (h *HeapProfileWritableHead) Discard() error {
	return h.head.Discard()
}

func NewHeapProfileWritableHead(head relabeler.Head, heapProfileWriter HeapProfileWriter) *HeapProfileWritableHead {
	return &HeapProfileWritableHead{head: head, heapProfileWriter: heapProfileWriter}
}
