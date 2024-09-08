-- name: BulkCreatePostLanguages :copyfrom
INSERT INTO post_languages (post_uri, language_id)
VALUES ($1, $2);
