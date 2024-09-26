package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
)

type PrometheusMiddleware struct {
	handler http.Handler
}

func (m *PrometheusMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/metrics" {
		// Skip collecting metrics from metrics endpoint itself
		m.handler.ServeHTTP(w, r)
		return
	}

	// begin timer to measure the requests duration
	timer := prometheus.NewTimer(HttpRequestDuration.WithLabelValues(path))

	// increment total request counter
	HttpRequestsTotal.WithLabelValues(path).Inc()

	// increment number of active connections
	ActiveConnections.Inc()

	// complete processing request
	m.handler.ServeHTTP(w, r)

	// record request duration (post processing)
	timer.ObserveDuration()

	// decrement total number of active connections (post processing)
	ActiveConnections.Dec()
}

func NewPrometheusMiddleware(handlerToWrap http.Handler) *PrometheusMiddleware {
	return &PrometheusMiddleware{handlerToWrap}
}
