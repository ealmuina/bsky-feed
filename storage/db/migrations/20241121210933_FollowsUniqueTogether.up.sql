ALTER TABLE follows
    ADD CONSTRAINT follows_uri_key_author_id_key UNIQUE (uri_key, author_id);