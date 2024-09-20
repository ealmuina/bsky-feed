DROP INDEX idx_interactions_created_at;
CREATE INDEX idx_interactions_created_at ON interactions (created_at DESC);

CREATE INDEX idx_interactions_cid ON interactions (cid DESC);