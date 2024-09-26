package monitoring

import "github.com/prometheus/client_golang/prometheus"

var (
	HttpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"path"},
	)

	HttpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)

	ActiveConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Number of active connections",
		},
	)

	FirehoseEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "firehose_events_total",
			Help: "Total number of events consumed from firehose",
		},
		[]string{"recordType"},
	)

	FirehoseEventProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "firehose_event_processing_duration_seconds",
			Help:    "Duration of firehose events processing",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"recordType"},
	)
)
