package manager

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/relabeler"
	"github.com/odarix/odarix-core-go/relabeler/config"
	"github.com/odarix/odarix-core-go/relabeler/head"
	"github.com/odarix/odarix-core-go/relabeler/head/catalog"
	"github.com/odarix/odarix-core-go/relabeler/logger"
	"github.com/odarix/odarix-core-go/util"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"path/filepath"
	"time"
)

type ConfigSource interface {
	Get() (inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16)
}

type Catalog interface {
	List(filter func(record catalog.Record) bool, sortLess func(lhs, rhs catalog.Record) bool) ([]catalog.Record, error)
	Create(id, dir string, numberOfShards uint16) (catalog.Record, error)
	SetStatus(id string, status catalog.Status) (catalog.Record, error)
	Delete(id string) error
}

type metrics struct {
	CreatedHeadsCount   prometheus.Counter
	RotatedHeadsCount   prometheus.Counter
	CorruptedHeadsCount prometheus.Counter
	PersistedHeadsCount prometheus.Counter
	DeletedHeadsCount   prometheus.Counter
}

type Manager struct {
	dir          string
	configSource ConfigSource
	catalog      Catalog
	generation   uint64
	counter      *prometheus.CounterVec
	registerer   prometheus.Registerer
}

func New(dir string, configSource ConfigSource, catalog Catalog, registerer prometheus.Registerer) (*Manager, error) {
	dirStat, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to stat dir: %w", err)
	}

	if !dirStat.IsDir() {
		return nil, fmt.Errorf("%s is not directory", dir)
	}

	factory := util.NewUnconflictRegisterer(registerer)

	return &Manager{
		dir:          dir,
		configSource: configSource,
		catalog:      catalog,
		counter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "opcore_head_event_count",
				Help: "Number of head events",
			},
			[]string{"type"},
		),
		registerer: registerer,
	}, nil
}

func (m *Manager) Restore(blockDuration time.Duration) (active relabeler.Head, rotated []relabeler.Head, err error) {
	headRecords, err := m.catalog.List(
		func(record catalog.Record) bool {
			return record.DeletedAt == 0 && record.Status != catalog.StatusCorrupted
		},
		func(lhs, rhs catalog.Record) bool {
			return lhs.CreatedAt < rhs.CreatedAt
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list head records: %w", err)
	}

	for index, headRecord := range headRecords {
		var h relabeler.Head
		headDir := filepath.Join(m.dir, headRecord.Dir)
		if headRecord.Status == catalog.StatusPersisted {
			if err = os.RemoveAll(headDir); err != nil {
				// todo: log
				continue
			}
			if err = m.catalog.Delete(headRecord.ID); err != nil {
				return nil, nil, fmt.Errorf("failed to delete record from catalog: %w", err)
			}

			m.counter.With(prometheus.Labels{"type": "deleted"}).Inc()

			continue
		}

		cfgs, _ := m.configSource.Get()
		h, err = head.Load(headRecord.ID, m.generation, headDir, cfgs, headRecord.NumberOfShards, m.registerer)
		if err != nil {
			_, setStatusErr := m.catalog.SetStatus(headRecord.ID, catalog.StatusCorrupted)
			if setStatusErr != nil {
				return nil, nil, errors.Join(err, setStatusErr)
			}
			m.counter.With(prometheus.Labels{"type": "corrupted"}).Inc()
			continue
		}
		m.generation++
		h = NewDiscardableRotatableHead(
			h,
			func(id string, err error) error {
				if _, rotateErr := m.catalog.SetStatus(id, catalog.StatusRotated); rotateErr != nil {
					return errors.Join(err, rotateErr)
				}
				m.counter.With(prometheus.Labels{"type": "rotated"}).Inc()
				return err
			},
			func(id string) error {
				var discardErr error
				if _, discardErr = m.catalog.SetStatus(id, catalog.StatusPersisted); discardErr != nil {
					return discardErr
				}
				m.counter.With(prometheus.Labels{"type": "persisted"}).Inc()
				if discardErr = os.RemoveAll(headDir); discardErr != nil {
					logger.Errorf("FAILED TO DELETE DIR", headDir, discardErr)
					return discardErr
				}
				if discardErr = m.catalog.Delete(id); discardErr != nil {
					return discardErr
				}
				m.counter.With(prometheus.Labels{"type": "deleted"}).Inc()
				return nil
			})
		m.counter.With(prometheus.Labels{"type": "created"}).Inc()
		if index == len(headRecords)-1 && time.Now().Sub(time.UnixMilli(headRecord.CreatedAt)).Milliseconds() < blockDuration.Milliseconds() {
			active = h
			continue
		}
		h.Finalize()
		if headRecord.Status != catalog.StatusRotated {
			if _, err = m.catalog.SetStatus(headRecord.ID, catalog.StatusRotated); err != nil {
				return nil, nil, fmt.Errorf("failed to set status: %w", err)
			}
			m.counter.With(prometheus.Labels{"type": "rotated"}).Inc()
		}
		rotated = append(rotated, h)
	}

	if active == nil {
		cfgs, numberOfShards := m.configSource.Get()
		active, err = m.BuildWithConfig(cfgs, numberOfShards)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build active head: %w", err)
		}
	}

	return active, rotated, nil
}

func (m *Manager) Build() (relabeler.Head, error) {
	cfgs, numberOfShards := m.configSource.Get()
	return m.BuildWithConfig(cfgs, numberOfShards)
}

func (m *Manager) BuildWithConfig(inputRelabelerConfigs []*config.InputRelabelerConfig, numberOfShards uint16) (h relabeler.Head, err error) {
	id := uuid.New().String()
	headDir := filepath.Join(m.dir, id)
	if err = os.Mkdir(headDir, 0777); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(headDir))
		}
	}()

	generation := m.generation

	h, err = head.Create(id, generation, headDir, inputRelabelerConfigs, numberOfShards, m.registerer)
	if err != nil {
		return nil, fmt.Errorf("failed to create head: %w", err)
	}

	if _, err = m.catalog.Create(id, id, numberOfShards); err != nil {
		return nil, err
	}

	m.generation++

	m.counter.With(prometheus.Labels{"type": "created"}).Inc()
	return NewDiscardableRotatableHead(
		h,
		func(id string, err error) error {
			if _, rotateErr := m.catalog.SetStatus(id, catalog.StatusRotated); rotateErr != nil {
				return errors.Join(err, rotateErr)
			}
			m.counter.With(prometheus.Labels{"type": "rotated"}).Inc()
			return err
		},
		func(id string) error {
			var discardErr error
			if _, discardErr = m.catalog.SetStatus(id, catalog.StatusPersisted); discardErr != nil {
				return discardErr
			}
			m.counter.With(prometheus.Labels{"type": "persisted"}).Inc()
			if discardErr = os.RemoveAll(headDir); discardErr != nil {
				return discardErr
			}
			if discardErr = m.catalog.Delete(id); discardErr != nil {
				return discardErr
			}
			m.counter.With(prometheus.Labels{"type": "deleted"}).Inc()
			return nil
		},
	), nil
}
