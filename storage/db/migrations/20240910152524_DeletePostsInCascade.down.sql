ALTER TABLE posts
    DROP CONSTRAINT posts_author_did_fkey,
    ADD CONSTRAINT posts_author_did_fkey
        FOREIGN KEY (author_did)
            REFERENCES users (did);