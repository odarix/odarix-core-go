package delivery

import "github.com/prometheus/client_golang/prometheus"

// ConflictRegisterer is a prometheus.Registerer wrap to avoid duplicates errors.
type ConflictRegisterer struct {
	prometheus.Registerer
}

// NewConflictRegisterer is the constructor.
func NewConflictRegisterer(r prometheus.Registerer) ConflictRegisterer {
	return ConflictRegisterer{r}
}

// NewCounter create new prometheus.Counter and register it in wrapped registerer.
func (cr ConflictRegisterer) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewCounter(opts))
}

// NewCounterVec create new prometheus.CounterVec and register it in wrapped registerer
func (cr ConflictRegisterer) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewCounterVec(opts, labelNames))
}

// NewGauge create new prometheus.Gauge and register it in wrapped registerer
func (cr ConflictRegisterer) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewGauge(opts))
}

// NewGaugeVec create new prometheus.GaugeVec and register it in wrapped registerer
func (cr ConflictRegisterer) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewGaugeVec(opts, labelNames))
}

// NewHistogramVec create new prometheus.HistogramVec and register it in wrapped registerer
func (cr ConflictRegisterer) NewHistogramVec(
	//nolint:gocritic // should be compatible with prometheus.NewHistogramVec
	opts prometheus.HistogramOpts, labelNames []string,
) *prometheus.HistogramVec {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewHistogramVec(opts, labelNames))
}

// NewHistogram create new prometheus.Histogram and register it in wrapped registerer
func (cr ConflictRegisterer) NewHistogram(
	//nolint:gocritic // should be compatible with prometheus.NewHistogramVec
	opts prometheus.HistogramOpts,
) prometheus.Histogram {
	return mustRegisterOrGet(cr.Registerer, prometheus.NewHistogram(opts))
}

func mustRegisterOrGet[Collector prometheus.Collector](r prometheus.Registerer, c Collector) Collector {
	if r == nil {
		return c
	}
	err := r.Register(c)
	if err == nil {
		return c
	}
	if arErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return arErr.ExistingCollector.(Collector)
	}
	panic(err)
}
