package appender

import (
	"context"
	"fmt"
	"sync"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/relabeler"
	"github.com/odarix/odarix-core-go/relabeler/querier"
	"github.com/prometheus/prometheus/storage"
)

type QueryableAppender struct {
	lock           sync.Mutex
	head           relabeler.Head
	distributor    relabeler.Distributor
	querierMetrics *querier.Metrics
}

func NewQueryableAppender(
	head relabeler.Head,
	distributor relabeler.Distributor,
	querierMetrics *querier.Metrics,
) *QueryableAppender {
	return &QueryableAppender{
		head:           head,
		distributor:    distributor,
		querierMetrics: querierMetrics,
	}
}

func (qa *QueryableAppender) Append(
	ctx context.Context,
	incomingData *relabeler.IncomingData,
	state *cppbridge.State,
	relabelerID string,
) error {
	return qa.AppendWithStaleNans(ctx, incomingData, state, relabelerID)
}

func (qa *QueryableAppender) AppendWithStaleNans(
	ctx context.Context,
	incomingData *relabeler.IncomingData,
	state *cppbridge.State,
	relabelerID string,
) error {
	qa.lock.Lock()
	defer qa.lock.Unlock()

	data, err := qa.head.Append(ctx, incomingData, state, relabelerID)
	if err != nil {
		return err
	}

	if err = qa.distributor.Send(ctx, qa.head, data); err != nil {
		return err
	}

	return nil
}

func (qa *QueryableAppender) WriteMetrics() {
	qa.lock.Lock()
	defer qa.lock.Unlock()

	qa.head.WriteMetrics()
	qa.distributor.WriteMetrics(qa.head)
}

func (qa *QueryableAppender) HeadStatus(limit int) relabeler.HeadStatus {
	qa.lock.Lock()
	defer qa.lock.Unlock()
	return qa.head.Status(limit)
}

func (qa *QueryableAppender) Rotate() error {
	qa.lock.Lock()
	defer qa.lock.Unlock()

	if err := qa.head.Rotate(); err != nil {
		return fmt.Errorf("failed to rotate head: %w", err)
	}

	if err := qa.distributor.Rotate(); err != nil {
		return fmt.Errorf("failed to rotate distributor: %w", err)
	}

	return nil
}

func (qa *QueryableAppender) Reconfigure(
	headConfigurator relabeler.HeadConfigurator,
	distributorConfigurator relabeler.DistributorConfigurator,
) error {
	qa.lock.Lock()
	defer qa.lock.Unlock()

	if err := headConfigurator.Configure(qa.head); err != nil {
		return fmt.Errorf("failed to reconfigure head: %w", err)
	}

	if err := distributorConfigurator.Configure(qa.distributor); err != nil {
		return fmt.Errorf("failed to upgrade distributor: %w", err)
	}

	return nil
}

func (qa *QueryableAppender) Querier(mint, maxt int64) (storage.Querier, error) {
	qa.lock.Lock()
	defer qa.lock.Unlock()
	head := qa.head
	return querier.NewQuerier(
		head,
		querier.NoOpShardedDeduplicatorFactory(),
		mint,
		maxt,
		func() error {
			return nil
		},
		qa.querierMetrics,
	), nil
}