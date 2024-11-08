package middleware

import (
	"bsky/monitoring"
	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
)

type OperationHandler func(*jsmodels.Event) error

type FirehoseMiddleware struct {
	handler OperationHandler
}

func (m *FirehoseMiddleware) HandleOperation(evt *jsmodels.Event) error {
	monitoring.FirehoseEvents.WithLabelValues(evt.Commit.Collection, evt.Commit.Operation).Inc()

	timer := prometheus.NewTimer(
		monitoring.FirehoseEventProcessingDuration.WithLabelValues(evt.Commit.Collection, evt.Commit.Operation),
	)
	err := m.handler(evt)
	timer.ObserveDuration()

	return err
}

func NewFirehoseMiddleware(handlerToWrap OperationHandler) *FirehoseMiddleware {
	return &FirehoseMiddleware{handlerToWrap}
}
