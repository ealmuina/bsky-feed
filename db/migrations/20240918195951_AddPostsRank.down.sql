ALTER TABLE posts
    DROP COLUMN rank,
    ADD COLUMN cid VARCHAR(255);

CREATE INDEX idx_posts_cid ON posts (cid DESC);