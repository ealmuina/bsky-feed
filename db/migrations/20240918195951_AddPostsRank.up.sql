ALTER TABLE posts
    ADD COLUMN rank FLOAT,
    DROP COLUMN cid;

CREATE INDEX idx_posts_rank ON posts (rank DESC);