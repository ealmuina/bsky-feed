ALTER TABLE interactions
    ADD COLUMN cid VARCHAR(255);

CREATE INDEX idx_interactions_cid ON interactions (cid);