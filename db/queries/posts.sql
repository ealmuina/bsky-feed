-- name: GetLanguagePosts :many
SELECT posts.*
FROM posts
         INNER JOIN post_languages pl on posts.uri = pl.post_uri
         INNER JOIN public.languages l on l.id = pl.language_id
WHERE l.code = $1
    AND reply_root IS NULL
    AND created_at < $2
   OR (created_at = $2 AND cid < $3)
ORDER BY created_at DESC, cid DESC
LIMIT $4;

-- name: BulkCreatePosts :copyfrom
INSERT INTO posts (uri, author_did, cid, reply_parent, reply_root, created_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: DeletePost :exec
DELETE
FROM posts
WHERE uri = $1;