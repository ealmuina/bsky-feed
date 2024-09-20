CREATE INDEX IF NOT EXISTS "idx_posts_reply_root" ON "posts" ("reply_root");
CREATE INDEX IF NOT EXISTS "idx_posts_cid" ON "posts" ("cid");
