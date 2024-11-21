DELETE
FROM follows
WHERE id NOT IN (SELECT MIN(id)
                 FROM follows
                 GROUP BY uri_key, author_id);

ALTER TABLE follows
    ADD CONSTRAINT follows_uri_key_author_id_key UNIQUE (uri_key, author_id);