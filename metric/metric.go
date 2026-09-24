package metric

import (
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const Name = "go_mongo_cdc_postgresql"

var (
	actionCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(Name, "postgresql_connector_action_total", "current"),
			Help: "PostgreSQL connector action counter",
		},
		[]string{"action_type", "result", "table_name"},
	)

	processLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "postgresql_connector_latency_ms", "current"),
			Help: "PostgreSQL connector latency ms",
		},
	)

	batchProcessLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "postgresql_connector_batch_process_latency_ms", "current"),
			Help: "PostgreSQL connector batch process latency ms",
		},
	)
)

type PrometheusMetricsRecorder struct{}

func NewMetricsRecorder() postgresql.MetricsRecorder {
	return &PrometheusMetricsRecorder{}
}

func (m *PrometheusMetricsRecorder) RecordUpsertSuccess(tableName string, count int64) {
	actionCounter.WithLabelValues("upsert", "success", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordUpsertError(tableName string, count int64) {
	actionCounter.WithLabelValues("upsert", "error", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordDeleteSuccess(tableName string, count int64) {
	actionCounter.WithLabelValues("delete", "success", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordDeleteError(tableName string, count int64) {
	actionCounter.WithLabelValues("delete", "error", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordRawSuccess(tableName string, count int64) {
	actionCounter.WithLabelValues("raw", "success", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordRawError(tableName string, count int64) {
	actionCounter.WithLabelValues("raw", "error", tableName).Add(float64(count))
}

func (m *PrometheusMetricsRecorder) RecordProcessLatency(latencyMs int64) {
	processLatencyGauge.Set(float64(latencyMs))
}

func (m *PrometheusMetricsRecorder) RecordBatchProcessLatency(latencyMs int64) {
	batchProcessLatencyGauge.Set(float64(latencyMs))
}
