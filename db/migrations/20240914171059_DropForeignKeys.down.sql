ALTER TABLE posts
    ALTER COLUMN author_did DROP NOT NULL,
    ADD CONSTRAINT posts_author_did_fkey
        FOREIGN KEY (author_did) REFERENCES users ON DELETE CASCADE;