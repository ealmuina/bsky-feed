package middleware

import (
	"bsky/monitoring"
	"context"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
)

type OperationHandler func(
	context.Context,
	*repo.Repo,
	*atproto.SyncSubscribeRepos_RepoOp,
	string,
) error

type FirehoseMiddleware struct {
	handler OperationHandler
}

func (m *FirehoseMiddleware) HandleOperation(
	ctx context.Context,
	rr *repo.Repo,
	op *atproto.SyncSubscribeRepos_RepoOp,
	commitRepo string,
) error {
	recordType := strings.Split(op.Path, "/")[0]

	monitoring.FirehoseEvents.WithLabelValues(recordType).Inc()

	timer := prometheus.NewTimer(monitoring.FirehoseEventProcessingDuration.WithLabelValues(recordType))
	err := m.handler(ctx, rr, op, commitRepo)
	timer.ObserveDuration()

	return err
}

func NewFirehoseMiddleware(handlerToWrap OperationHandler) *FirehoseMiddleware {
	return &FirehoseMiddleware{handlerToWrap}
}
