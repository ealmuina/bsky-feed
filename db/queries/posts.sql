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
-- Reposts from top accounts
(SELECT i.post_uri as uri,
        i.uri      as repost_uri,
        i.created_at,
        i.cid
 FROM interactions i
          INNER JOIN users u ON author_did = u.did
          INNER JOIN posts p ON post_uri = p.uri
 WHERE p.language = $1
   AND u.followers_count > 1000
   AND (i.created_at < $2 OR (i.created_at = $2 AND i.cid < $3))
 ORDER BY i.created_at DESC, i.cid DESC
 LIMIT $4)
UNION
-- Posts from top accounts
(SELECT uri as uri,
        ''  as repost_uri,
        created_at,
        cid
 FROM posts
          INNER JOIN users u ON posts.author_did = u.did
 WHERE language = $1
   AND reply_root IS NULL
   AND u.followers_count > 1000
   AND (created_at < $2 OR (created_at = $2 AND cid < $3))
 ORDER BY created_at DESC, cid DESC
 LIMIT $4)
ORDER BY created_at DESC, cid DESC
LIMIT $4;
