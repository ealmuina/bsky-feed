ALTER TABLE posts
    DROP CONSTRAINT posts_author_did_fkey,
    ALTER COLUMN author_did SET NOT NULL;