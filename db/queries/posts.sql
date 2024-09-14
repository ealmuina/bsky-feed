-- name: BulkCreatePosts :copyfrom
INSERT INTO posts (uri, author_did, cid, reply_parent, reply_root, created_at, language)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: BulkDeletePosts :exec
DELETE
FROM posts
WHERE uri = ANY ($1::VARCHAR[]);

-- name: DeleteOldPosts :exec
DELETE
FROM posts
WHERE indexed_at < current_timestamp - interval '10 days';

-- name: DeleteUserPosts :exec
DELETE
FROM posts
WHERE author_did = $1;

-- name: GetLanguagePosts :many
SELECT posts.*
FROM posts
WHERE language = $1
  AND reply_root IS NULL
  AND (created_at < $2 OR (created_at = $2 AND cid < $3))
ORDER BY created_at DESC, cid DESC
LIMIT $4;

-- name: GetLanguageTopPosts :many
WITH top_reposts AS (SELECT post_uri, uri repost_uri
                     FROM interactions
                              INNER JOIN users u ON author_did = u.did
                     WHERE u.followers_count > 1000
                       AND kind = 'repost')
SELECT posts.*, repost_uri
FROM posts
         INNER JOIN users u ON author_did = u.did
         LEFT JOIN top_reposts tr ON uri = tr.post_uri
WHERE language = $1
  AND reply_root IS NULL
  AND (u.followers_count > 1000 or tr.repost_uri IS NOT NULL)
  AND (posts.created_at < $2 OR (posts.created_at = $2 AND posts.cid < $3))
ORDER BY posts.created_at DESC, posts.cid DESC
LIMIT $4;
