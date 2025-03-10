package remotewriter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/relabeler/head/catalog"
	"github.com/odarix/odarix-core-go/relabeler/logger"
	"github.com/prometheus/prometheus/storage/remote"
)

const defaultDelay = time.Second * 5

type writeLoop struct {
	dataDir       string
	destination   *Destination
	currentHeadID *string
	catalog       Catalog
	clock         clockwork.Clock
	client        remote.WriteClient
}

func newWriteLoop(dataDir string, destination *Destination, catalog Catalog, clock clockwork.Clock) *writeLoop {
	return &writeLoop{
		dataDir:     dataDir,
		destination: destination,
		catalog:     catalog,
		clock:       clock,
	}
}

func (wl *writeLoop) run(ctx context.Context) {
	var delay time.Duration
	var err error
	var i *Iterator
	var nextI *Iterator

	rw := &readyProtobufWriter{}

	wl.destination.metrics.maxNumShards.Set(float64(wl.destination.Config().QueueConfig.MaxShards))
	wl.destination.metrics.minNumShards.Set(float64(wl.destination.Config().QueueConfig.MinShards))

	defer func() {
		if i != nil {
			_ = i.Close()
		}
		if nextI != nil {
			_ = nextI.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wl.clock.After(delay):
			delay = 0
		}

		if i == nil {
			if nextI != nil {
				i = nextI
				nextI = nil
			} else {
				i, err = wl.nextIterator(ctx, rw)
				if err != nil {
					logger.Errorf("failed to get current next iterator: %v", err)
					delay = defaultDelay
					continue
				}
			}
		}

		if wl.client == nil {
			wl.client, err = createClient(wl.destination.Config())
			if err != nil {
				logger.Errorf("failed to create client: %v", err)
				delay = defaultDelay
				continue
			}

			rw.SetProtobufWriter(newProtobufWriter(wl.client))
		}

		if err = wl.write(ctx, i); err != nil {
			logger.Errorf("failed to write iterator: %v", err)
			delay = defaultDelay
			continue
		}

		if nextI == nil {
			nextI, err = wl.nextIterator(ctx, rw)
			if err != nil {
				logger.Errorf("failed to get next iterator: %v", err)
				delay = defaultDelay
				continue
			}
		}

		if err = i.Close(); err != nil {
			logger.Errorf("failed to close iterator: %v", err)
			delay = defaultDelay
			continue
		}

		i = nil
	}
}

func createClient(config DestinationConfig) (client remote.WriteClient, err error) {
	clientConfig := remote.ClientConfig{
		URL:              config.URL,
		Timeout:          config.RemoteTimeout,
		HTTPClientConfig: config.HTTPClientConfig,
		SigV4Config:      config.SigV4Config,
		AzureADConfig:    config.AzureADConfig,
		Headers:          config.Headers,
		RetryOnRateLimit: true,
	}

	client, err = remote.NewWriteClient(config.Name, &clientConfig)
	if err != nil {
		return nil, fmt.Errorf("falied to create client: %w", err)
	}

	return client, nil
}

func (wl *writeLoop) write(ctx context.Context, iterator *Iterator) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := iterator.Next(ctx)
			if err != nil {
				if errors.Is(err, ErrEndOfBlock) {
					return nil
				}
				logger.Errorf("iteration failed: %v", err)
			}
		}

	}
}

func (wl *writeLoop) nextIterator(ctx context.Context, protobufWriter ProtobufWriter) (*Iterator, error) {
	var nextHeadRecord *catalog.Record
	var err error
	var cleanStart bool
	if wl.currentHeadID != nil {
		nextHeadRecord, err = nextHead(ctx, wl.catalog, *wl.currentHeadID)
	} else {
		var headFound bool
		nextHeadRecord, headFound, err = scanForNextHead(ctx, wl.dataDir, wl.catalog, wl.destination.Config().Name)
		cleanStart = !headFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find next head: %w", err)
	}
	headDir := filepath.Join(wl.dataDir, nextHeadRecord.Dir())
	crw, err := NewCursorReadWriter(filepath.Join(headDir, fmt.Sprintf("%s.cursor", wl.destination.Config().Name)), nextHeadRecord.NumberOfShards())
	if err != nil {
		return nil, fmt.Errorf("failed to create cursor: %w", err)
	}

	crc32, err := wl.destination.Config().CRC32()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to calculate crc32: %w", err), crw.Close())
	}

	var discardCache bool
	if crw.GetConfigCRC32() != crc32 {
		if err = crw.SetConfigCRC32(crc32); err != nil {
			return nil, errors.Join(fmt.Errorf("failed to write crc32: %w", err), crw.Close())
		}
		discardCache = true
	}

	ds, err := newDataSource(
		headDir,
		nextHeadRecord.NumberOfShards(),
		wl.destination.Config(),
		discardCache,
		newSegmentReadyChecker(nextHeadRecord),
		wl.makeCorruptMarker(),
		nextHeadRecord,
		wl.destination.metrics.unexpectedEOFCount,
		wl.destination.metrics.segmentSizeInBytes,
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to create data source: %w", err), crw.Close())
	}

	headID := nextHeadRecord.ID()
	ds.ID = headID

	var targetSegmentID uint32
	if cleanStart {
		if nextHeadRecord.LastAppendedSegmentID() != nil {
			targetSegmentID = *nextHeadRecord.LastAppendedSegmentID()
		} else {
			targetSegmentID = crw.GetTargetSegmentID()
		}
	} else {
		targetSegmentID = crw.GetTargetSegmentID()
	}

	i, err := newIterator(
		wl.clock,
		wl.destination.Config().QueueConfig,
		ds,
		crw,
		targetSegmentID,
		wl.destination.Config().ReadTimeout,
		protobufWriter,
		wl.destination.metrics,
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to create data source: %w", err), crw.Close(), ds.Close())
	}

	wl.currentHeadID = &headID

	return i, nil
}

type CorruptMarkerFn func(headID string) error

func (fn CorruptMarkerFn) MarkCorrupted(headID string) error {
	return fn(headID)
}

func (wl *writeLoop) makeCorruptMarker() CorruptMarker {
	return CorruptMarkerFn(func(headID string) error {
		_, err := wl.catalog.SetCorrupted(headID)
		return err
	})
}

func nextHead(ctx context.Context, headCatalog Catalog, headID string) (*catalog.Record, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}

	headRecords, err := headCatalog.List(
		nil,
		func(lhs, rhs *catalog.Record) bool {
			return lhs.CreatedAt() < rhs.CreatedAt()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list head records: %w", err)
	}

	if len(headRecords) == 0 {
		return nil, fmt.Errorf("nextHead: no new heads: empty head records")
	}

	for index, headRecord := range headRecords {
		if headRecord.ID() == headID {
			if index == len(headRecords)-1 {
				return nil, fmt.Errorf("no new heads: last head record: %s", headID)
			}

			return headRecords[index+1], nil
		}
	}

	// unknown head id, selecting last head
	return headRecords[len(headRecords)-1], nil
}

func scanForNextHead(ctx context.Context, dataDir string, headCatalog Catalog, destinationName string) (*catalog.Record, bool, error) {
	if err := contextErr(ctx); err != nil {
		return nil, false, err
	}

	headRecords, err := headCatalog.List(
		nil,
		func(lhs, rhs *catalog.Record) bool {
			return lhs.CreatedAt() > rhs.CreatedAt()
		},
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to list head records: %w", err)
	}

	if len(headRecords) == 0 {
		return nil, false, fmt.Errorf("scanForNextHead: no new heads: empty head records")
	}

	var headFound bool
	for _, headRecord := range headRecords {
		headFound, err = scanHeadForDestination(filepath.Join(dataDir, headRecord.Dir()), destinationName)
		if err != nil {
			if !headRecord.Corrupted() {
				logger.Errorf("head %s is corrupted: %v", headRecord.ID(), err)
				if _, corruptErr := headCatalog.SetCorrupted(headRecord.ID()); corruptErr != nil {
					logger.Errorf("failed to set corrupted state: %v", corruptErr)
				}
			}

			continue
		}
		if headFound {
			return headRecord, true, nil
		}
	}

	// track of the previous destination not found, selecting last head
	return headRecords[0], false, nil
}

func scanHeadForDestination(dirPath string, destinationName string) (bool, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return false, fmt.Errorf("failed to open head dir: %w", err)
	}
	defer func() { _ = dir.Close() }()

	fileNames, err := dir.Readdirnames(-1)
	if err != nil {
		return false, fmt.Errorf("failed to read dir names: %w", err)
	}

	for _, fileName := range fileNames {
		if fileName == fmt.Sprintf("%s.cursor", destinationName) {
			return true, nil
		}
	}

	return false, nil
}

func contextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type readyProtobufWriter struct {
	protobufWriter ProtobufWriter
}

func (rpw *readyProtobufWriter) SetProtobufWriter(protobufWriter ProtobufWriter) {
	rpw.protobufWriter = protobufWriter
}

func (rw *readyProtobufWriter) Write(ctx context.Context, protobuf *cppbridge.SnappyProtobufEncodedData) error {
	return rw.protobufWriter.Write(ctx, protobuf)
}
