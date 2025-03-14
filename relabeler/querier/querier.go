package querier

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/model"
	"github.com/odarix/odarix-core-go/relabeler"
	"github.com/odarix/odarix-core-go/relabeler/logger"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type Deduplicator interface {
	Add(shard uint16, values ...string)
	Values() []string
}

type DeduplicatorFactory interface {
	Deduplicator(numberOfShards uint16) Deduplicator
}

type Querier struct {
	mint                int64
	maxt                int64
	head                relabeler.Head
	deduplicatorFactory DeduplicatorFactory
	closer              func() error
	metrics             *Metrics
}

func NewQuerier(head relabeler.Head, deduplicatorFactory DeduplicatorFactory, mint, maxt int64, closer func() error, metrics *Metrics) *Querier {
	return &Querier{
		mint:                mint,
		maxt:                maxt,
		head:                head,
		deduplicatorFactory: deduplicatorFactory,
		closer:              closer,
		metrics:             metrics,
	}
}

func (q *Querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	start := time.Now()
	defer func() {
		if q.metrics != nil {
			q.metrics.LabelValuesDuration.With(
				prometheus.Labels{"generation": fmt.Sprintf("%d", q.head.Generation())},
			).Observe(float64(time.Since(start).Milliseconds()))
		}
	}()

	dedup := q.deduplicatorFactory.Deduplicator(q.head.NumberOfShards())
	convertedMatchers := convertPrometheusMatchersToOpcoreMatchers(matchers...)

	err := q.head.ForEachShard(func(shard relabeler.Shard) error {
		queryLabelValuesResult := shard.LSS().QueryLabelValues(name, convertedMatchers)
		if queryLabelValuesResult.Status() != cppbridge.LSSQueryStatusMatch {
			return fmt.Errorf("no matches on shard: %d", shard.ShardID())
		}

		dedup.Add(shard.ShardID(), queryLabelValuesResult.Values()...)
		runtime.KeepAlive(queryLabelValuesResult)
		return nil
	})

	anns := *annotations.New()
	if err != nil {
		anns.Add(err)
	}

	select {
	case <-ctx.Done():
		return nil, anns, context.Cause(ctx)
	default:
	}

	labelValues := dedup.Values()
	sort.Strings(labelValues)

	return labelValues, anns, nil
}

func (q *Querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	start := time.Now()
	defer func() {
		if q.metrics != nil {
			q.metrics.LabelNamesDuration.With(
				prometheus.Labels{"generation": fmt.Sprintf("%d", q.head.Generation())},
			).Observe(float64(time.Since(start).Milliseconds()))
		}
	}()

	dedup := q.deduplicatorFactory.Deduplicator(q.head.NumberOfShards())
	convertedMatchers := convertPrometheusMatchersToOpcoreMatchers(matchers...)

	err := q.head.ForEachShard(func(shard relabeler.Shard) error {
		queryLabelNamesResult := shard.LSS().QueryLabelNames(convertedMatchers)
		if queryLabelNamesResult.Status() != cppbridge.LSSQueryStatusMatch {
			return fmt.Errorf("no matches on shard: %d", shard.ShardID())
		}

		dedup.Add(shard.ShardID(), queryLabelNamesResult.Names()...)
		runtime.KeepAlive(queryLabelNamesResult)
		return nil
	})

	anns := *annotations.New()
	if err != nil {
		anns.Add(err)
	}

	select {
	case <-ctx.Done():
		return nil, anns, context.Cause(ctx)
	default:
	}

	labelNames := dedup.Values()
	sort.Strings(labelNames)

	return labelNames, anns, nil
}

func (q *Querier) Close() error {
	if q.closer != nil {
		return q.closer()
	}

	return nil
}

func (q *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	start := time.Now()
	defer func() {
		if q.metrics != nil {
			q.metrics.SelectDuration.With(
				prometheus.Labels{"generation": fmt.Sprintf("%d", q.head.Generation())},
			).Observe(float64(time.Since(start).Milliseconds()))
		}
	}()

	seriesSets := make([]storage.SeriesSet, q.head.NumberOfShards())
	convertedMatchers := convertPrometheusMatchersToOpcoreMatchers(matchers...)
	callerID := cppbridge.GetCaller(ctx)

	err := q.head.ForEachShard(func(shard relabeler.Shard) error {
		lssQueryResult := shard.LSS().Query(convertedMatchers, callerID)

		if lssQueryResult.Status() != cppbridge.LSSQueryStatusMatch {
			seriesSets[shard.ShardID()] = &SeriesSet{}
			if lssQueryResult.Status() == cppbridge.LSSQueryStatusNoMatch {
				return nil
			}
			return fmt.Errorf("failed to query from shard: %d, query status: %d", shard.ShardID(), lssQueryResult.Status())
		}

		serializedChunks := shard.DataStorage().Query(cppbridge.HeadDataStorageQuery{
			StartTimestampMs: q.mint,
			EndTimestampMs:   q.maxt,
			LabelSetIDs:      lssQueryResult.Matches(),
		})

		if serializedChunks.NumberOfChunks() == 0 {
			seriesSets[shard.ShardID()] = &SeriesSet{}
			return nil
		}

		chunksIndex := serializedChunks.MakeIndex()
		getLabelSetsResult := shard.LSS().GetLabelSets(lssQueryResult.Matches())

		labelSetBySeriesID := make(map[uint32]cppbridge.Labels)
		for index, labelSetID := range lssQueryResult.Matches() {
			if chunksIndex.Has(labelSetID) {
				labelSetBySeriesID[labelSetID] = getLabelSetsResult.LabelsSets()[index]
			}
		}

		localSeriesSets := make([]*Series, 0, chunksIndex.Len())
		deserializer := cppbridge.NewHeadDataStorageDeserializer(serializedChunks)
		for _, seriesID := range lssQueryResult.Matches() {
			chunksMetadata := chunksIndex.Chunks(serializedChunks, seriesID)
			if len(chunksMetadata) == 0 {
				continue
			}

			localSeriesSets = append(localSeriesSets, &Series{
				seriesID: seriesID,
				mint:     q.mint,
				maxt:     q.maxt,
				labelSet: cloneLabelSet(labelSetBySeriesID[seriesID]),
				sampleProvider: &DefaultSampleProvider{
					deserializer:   deserializer,
					chunksMetadata: chunksMetadata,
				},
			})
		}
		runtime.KeepAlive(getLabelSetsResult)
		runtime.KeepAlive(lssQueryResult)

		seriesSets[shard.ShardID()] = NewSeriesSet(localSeriesSets)
		return nil
	})
	if err != nil {
		logger.Warnf("QUERIER: Select failed: %s", err)
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

func convertPrometheusMatchersToOpcoreMatchers(matchers ...*labels.Matcher) []model.LabelMatcher {
	opcoreMatchers := make([]model.LabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		opcoreMatchers = append(opcoreMatchers, model.LabelMatcher{
			Name:        matcher.Name,
			Value:       matcher.Value,
			MatcherType: uint8(matcher.Type),
		})
	}

	return opcoreMatchers
}

func cloneLabelSet(labelSet cppbridge.Labels) labels.Labels {
	builder := labels.NewScratchBuilder(len(labelSet))
	for i := range labelSet {
		builder.Add(labelSet[i].Name, labelSet[i].Value)
	}
	return builder.Labels()
}
