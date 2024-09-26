package middleware

import (
	"bsky/monitoring"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
)

type ServerMiddleware struct {
	handler http.Handler
}

func (m *ServerMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// increment total request counter
	monitoring.HttpRequestsTotal.WithLabelValues(path).Inc()

	// increment number of active connections
	monitoring.ActiveConnections.Inc()

	// begin timer to measure the requests duration
	timer := prometheus.NewTimer(monitoring.HttpRequestDuration.WithLabelValues(path))

	// complete processing request
	m.handler.ServeHTTP(w, r)

	// record request duration (post processing)
	timer.ObserveDuration()

	// decrement total number of active connections (post processing)
	monitoring.ActiveConnections.Dec()
}

func NewServerMiddleware(handlerToWrap http.Handler) *ServerMiddleware {
	return &ServerMiddleware{handlerToWrap}
}
