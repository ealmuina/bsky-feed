DROP INDEX idx_interactions_cid;

DROP INDEX idx_interactions_created_at;
CREATE INDEX idx_interactions_created_at ON interactions (created_at);